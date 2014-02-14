from collections import deque
import random
import sched
from threading import Thread, Event
import threading
import time
from GA.DEAPGA.GAImplementation.GAImplementation import construct_ga_alg, Params
from core.executors.EventMachine import EventMachine, TaskStart, TaskFinished, NodeFailed, NodeUp
from environment.Resource import Node
from environment.ResourceManager import ScheduleItem, Schedule
from environment.Utility import Utility


class GaHeftExecutor(EventMachine):
    def __init__(self,
                 heft_planner,
                 base_fail_duration,
                 base_fail_dispersion,
                 fixed_interval_for_ga):
        self.queue = deque()
        self.current_time = 0
        # DynamicHeft
        # both planners have acess to resource manager and estimator
        self.heft_planner = heft_planner
        #self.ga_planner = ga_planner
        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        self.current_schedule = None
        self.fixed_interval_for_ga = fixed_interval_for_ga

        #pair (front_event, GACalculation)
        #self._ga_calculation = None
        self.ga_computation_manager = GAComputationManager(self.fixed_interval_for_ga,
                                                           heft_planner.workflow,
                                                           heft_planner.resource_manager,
                                                           heft_planner.estimator)

        pass

    def init(self):

        ##TODO: get_nodes must be either common for both planners or does not be used in this executor at all
        ## create initial schedule with ga
        self.current_schedule = Schedule({node: [] for node in self.heft_planner.get_nodes()})

        #TODO: correct this hack later.
        #result = self.ga_computation_manager.run(self.current_schedule, self.current_time, False)
        result = self.ga_computation_manager._get_ga_alg()(self.current_schedule, None)
        self.current_schedule = result[2]

        self._post_new_events()

    def event_arrived(self, event):
        # existed types of events:
        # TaskStart, TaskFinished, NodeFailed, NodeUp

        #TODO: catch events and
        # case 1: critical event(resource failed or up, ga_rescheduling isn't started)
        #   - get fixed interval
        #   - get cleaned schedule
        #   - run HEFT rescheduling
        #   - build the Front Line
        #   - run ga_rescheduling with timer
        #   - continue execution

        # case 2: critical event(resource failed or up, ga_rescheduling is started)
        #   - wait until current time
        #   - interrupt timer (or interrupt without waiting)
        #   - rerun procedure from case 1
        #   - continue execution

        # case 3: critical event(the earliest point of the front line have been met, ga_rescheduling is started)
        #   - wait until current time
        #   - apply generated by GA results
        #   - continue execution

        # case 4: critical event(the earliest point of the front line have been met, ga_rescheduling is started)
        #   - wait until current time
        #   - if results generated by GA is not enough good, repeat case 1
        #   - continue execution

        # case 5: non-critical event (task started, task finished)
        #   - continue execution

        if isinstance(event, TaskStart):
            self._task_start_handler(event)
            return
        if isinstance(event, TaskFinished):
            self._task_finished_handler(event)
            return
        if isinstance(event, NodeFailed):
            self._node_failed_handler(event)
            return
        if isinstance(event, NodeUp):
            self._node_up_handler(event)
            return
        pass



    def _task_start_handler(self, event):

        res = self._check_event_for_ga_result(event)
        if res:
            return

        def check_fail(task, node):
            reliability = self.heft_planner.estimator.estimate_reliability(task, node)
            res = random.random()
            if res > reliability:
                return True
            return False
        # check task as executing
        # self.current_schedule.change_state(event.task, ScheduleItem.EXECUTING)
        # try to find nodes in cloud
        # check if failed and post
        (node, item) = self.current_schedule.place_by_time(event.task, event.time_happened)
        item.state = ScheduleItem.EXECUTING

        if check_fail(event.task, node):
            # generate fail time, post it
            duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
            time_of_fail = (item.end_time - self.current_time)*random.random()
            time_of_fail = self.current_time + (time_of_fail if time_of_fail > 0 else 0.01) ##(item.end_time - self.current_time)*0.01

            event_failed = NodeFailed(node, event.task)
            event_failed.time_happened = time_of_fail

            event_nodeup = NodeUp(node)
            event_nodeup.time_happened = time_of_fail + duration

            self.post(event_failed)
            self.post(event_nodeup)
            # remove TaskFinished event
            ##TODO: make a function for this purpose in the base class
            self.queue = deque([ev for ev in self.queue if not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id)])
        pass

    def _task_finished_handler(self, event):

        # check task finished
        self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)

        self._check_event_for_ga_result(event)

        pass

    def _node_failed_handler(self, event):

        ## interrupt ga
        self.ga_computation_manager.stop_and_get_result_now(self.current_time)

        # check node down
        self.heft_planner.resource_manager.node(event.node).state = Node.Down
        # check failed event in schedule
        ## TODO: ambigious choice
        ##self.current_schedule.change_state(event.task, ScheduleItem.FAILED)
        it = [item for item in self.current_schedule.mapping[event.node] if item.job.id == event.task.id and item.state == ScheduleItem.EXECUTING]
        if len(it) != 1:
            ## TODO: raise exception here
            pass

        it[0].state = ScheduleItem.FAILED
        it[0].end_time = self.current_time

        # run HEFT
        self._reschedule(event)

        #run GA
        self.ga_computation_manager.run(self.current_schedule, self.current_time)

        pass

    def _node_up_handler(self, event):
        ## interrupt ga
        self.ga_computation_manager.stop_and_get_result_now(self.current_time)

        # check node up
        self.heft_planner.resource_manager.node(event.node).state = Node.Unknown
        self._reschedule(event)

         #run GA
        self.ga_computation_manager.run(self.current_schedule, self.current_time)
        pass

    def _reschedule(self, event):
        self.heft_planner.current_time = self.current_time
        current_cleaned_schedule = self._clean_events(event)
        self.current_schedule = self.heft_planner.run(current_cleaned_schedule)
        self._post_new_events()

    def _post_new_events(self):
        unstarted_items = set()
        for (node, items) in self.current_schedule.mapping.items():
            for item in items:
                if item.state == ScheduleItem.UNSTARTED:
                    unstarted_items.add(item)

        events_to_post = []
        for item in unstarted_items:
            event_start = TaskStart(item.job)
            event_start.time_happened = item.start_time

            event_finish = TaskFinished(item.job)
            event_finish.time_happened = item.end_time

            events_to_post
            self.post(event_start)
            self.post(event_finish)
        pass

    def _clean_events(self, event):

        # remove all unstarted tasks
        cleaned_task = set()
        if isinstance(event, NodeFailed):
            cleaned_task = set([event.task])

        new_mapping = dict()
        for (node, items) in self.current_schedule.mapping.items():
            new_mapping[node] = []
            for item in items:
                if item.state != ScheduleItem.UNSTARTED:
                    new_mapping[node].append(item)
                else:
                    cleaned_task.add(item.job)
        clean_schedule = Schedule(new_mapping)
        # remove all events associated with these tasks
        def check(event):
            if isinstance(event, TaskStart) and event.task in cleaned_task:
                return False
            if isinstance(event, TaskFinished) and event.task in cleaned_task:
                return False
            return True
        ##TODO: refactor it later
        self.queue = deque([event for event in self.queue if check(event)])
        return clean_schedule

    def _check_event_for_ga_result(self, event):
        # check for time to get result from GA running background
        result = self.ga_computation_manager.check_result(event, self.current_time)
        if result is not None:
            resulted_schedule = result
            t1 = Utility.get_the_last_time(resulted_schedule)
            t2 = Utility.get_the_last_time(self.current_schedule)
            ## TODO: uncomment it later.
            if True:#t1 < t2:
                ## generate new events
                self._replace_current_schedule(event, resulted_schedule)
                ## if event is TaskStarted event the return value means skip further processing
                return True
            else:
                ## TODO: run_ga_yet_another_with_old_genome
                self.ga_computation_manager.run(self.current_schedule, self.current_time)
        return False


    def _replace_current_schedule(self, event, new_schedule):
        # syncrhonize fixed part of new_schedule with the old schedule - lets assume new_schedule already synchonized
        # remove all events related with the old schedule
        # replace current with new
        # generate events of new schedule and post their

        self._clean_events(event)
        self.current_schedule = new_schedule
        self._post_new_events()
        pass
    pass

## key responsibilities:
##  - run ga computation in sync and async ways
##  - interrupting by timer or external event
##  - providing information about status of computation
##  - encapsulating some action - ? (for the sake of simplicity it must be responsibility of GaHeftExecutor)
class GAComputationManager:

    def __init__(self,
                 fixed_interval_for_ga,
                 workflow,
                 resource_manager,
                 estimator):
        self.fixed_interval_for_ga = fixed_interval_for_ga

        self.workflow = workflow
        self.resource_manager = resource_manager
        self.estimator = estimator

        self.current_computation = None
        pass

    def _clean_current_computation(self):
        self.current_computation = None

    ## check result of ga computation if it appropriate event
    def check_result(self, event, current_time):

        if self.current_computation is None:
            return None

        #if self.current_computation[0].job.id == event.task.id or self.current_computation[0].end_time == current_time or self.current_computation[0].start_time:
        if self.current_computation[0].end_time == current_time:
             # we estimate the rest time of ga calculation and wait it
             result = self._get_result_until_current_time(current_time)
             ## TODO: redesign this later
             return result[2]

        return None

    def stop_and_get_result_now(self, current_time):
        if self.current_computation is None:
            return None
        print("Time: " + str(current_time) + " run execution due to something ")
        ##TODO: just clean it for now. perhaps, we can't use existed results so drop it
        self._clean_current_computation()
        ##result = self._get_result_until_current_time(current_time)
        return None

    def run(self, current_schedule, current_time, async=True):
        self._reschedule_by_GA(current_schedule, current_time)
        if not async:
            result = self.current_computation[1].ga(self.current_computation[1].fixed_schedule_part, None)
            self._clean_current_computation()
            return result
        return None

    def _reschedule_by_GA(self, current_schedule, current_time):
        print("Time: " + str(current_time) + " Creating reschedule point ")
        ## TODO: how about several events in one time?
        def _get_front_line(schedule, current_time, fixed_interval):
            event_time = current_time + fixed_interval
            ## TODO: remake it later
            min_item = ScheduleItem(None, 1000000, 1000000)

            for (node, items) in schedule.mapping.items():
                for item in items:
                    ## It accounts case when event_time appears in a transfer gap(rare situation for all nodes)
                    if event_time < item.end_time and item.end_time < min_item.end_time:
                        min_item = item
                        break

            if min_item.job is None:
                return None
            print("Time: " + str(current_time) + " reschedule point have been founded st:" + str(min_item.start_time) + " end:" + str(min_item.end_time))
            return min_item

        def _get_fixed_schedule(schedule, front_event):
            def is_before_event(item):
                # hard to resolve corner case. The simulator doesn't guranteed the order of appearing events.
                if item.start_time < front_event.end_time:
                    return True
                return False
            ##TODO: it's dangerous operation.
            ## TODO: need create new example of ScheduleItem.
            def set_proper_state(item):

                new_item = ScheduleItem.copy(item)

                non_finished = new_item.state == ScheduleItem.EXECUTING or new_item.state == ScheduleItem.UNSTARTED
                ## TODO: Urgent!: dangerous place
                if non_finished and new_item.end_time <= front_event.end_time:
                    new_item.state = ScheduleItem.FINISHED
                if non_finished and new_item.end_time > front_event.end_time:
                    new_item.state = ScheduleItem.EXECUTING
                return new_item
            if front_event.job is None:
                ##TODO: I don't for now what to do
                #print("GA's computation isn't able to meet the end of computation")
                return schedule
            else:
                fixed_mapping = {key: [set_proper_state(item) for item in items if is_before_event(item)] for (key, items) in schedule.mapping.items()}
                return Schedule(fixed_mapping)

        ## TODO: make previous_result used
        def run_ga(current_schedule):
            fixed_interval = self.fixed_interval_for_ga
            front_event = _get_front_line(current_schedule, current_time, fixed_interval)
            # we can't meet the end of computation so we do nothing
            if front_event is None:
                print("GA's computation isn't able to meet the end of computation")
                return
            fixed_schedule = _get_fixed_schedule(current_schedule, front_event)


            count = len(fixed_schedule.get_all_unique_tasks_id())
            if count == 30:
                return
                #k = 0
                #raise Exception("Look here!")

            #_run_in_parallel_with_timer(fixed_schedule)
            #_register_line_events(front_event)
            ## TODO: replace it with modified GAComputation from GAImplementation
            ##ga_calc = GACalculation(self.fixed_interval_for_ga, fixed_schedule)...
            ga = self._get_ga_alg()

            ga_calc = GAComputationWrapper(ga, fixed_schedule, None, current_time)

            self.current_computation = (front_event, ga_calc, )
            pass

        if not self._is_ga_running():
            run_ga(current_schedule)
        else:
            result = self.current_computation[1].stop()
            self._clean_current_computation()
            run_ga(current_schedule)
        pass

    def _get_ga_alg(self):
        ga = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=Params(20, 2, 0.8, 0.5, 0.4, 2))
        return ga

    ## actual run happens here
    def _get_result_until_current_time(self, current_time):
        ga_calc = self.current_computation[1]
        ## TODO: blocking call with complete calculation
        time_interval = current_time - ga_calc.creation_time
        result = ga_calc.run(time_interval, current_time)
        self._clean_current_computation()
        return result

    def _is_ga_running(self):
        if self.current_computation is None:
            return False
        return self.current_computation[1].is_running()
    pass

class GAComputationWrapper:

    def __init__(self, ga, fixed_schedule_part, initial_schedule, creation_time):
        self.ga = ga
        self.fixed_schedule_part = fixed_schedule_part
        self.initial_schedule = initial_schedule
        self.creation_time = creation_time
        pass

    def run(self, time_interval, current_time):

        event = Event()

        def go():
            event.set()
            self.ga.stop()
            pass

        timer = threading.Timer(time_interval, go)

        def run_func():
            t_ident = str(threading.current_thread().ident)
            t_name = str(threading.current_thread().name)
            print("Time: " + str(current_time) + " Running ga in isolated thread " + t_name + " " + t_ident)
            self.ga(self.fixed_schedule_part,
                    self.initial_schedule,
                    current_time)
            event.set()
            timer.cancel()
            pass

        t = Thread(target=run_func)
        t.start()


        timer.start()
        print("Time: " + str(current_time) + " waiting for thread " + str(t.name) + " " + str(t.ident))
        event.wait()
        print("Time: " + str(current_time) + " thread finished " + str(t.name) + " " + str(t.ident))

        result = self.ga.get_result()

        resulted_schedule = result[2]

        ## TODO: Urgent! make a check for consistency with fixed schedule
        fpart_check = Utility.check_fixed_part(resulted_schedule, self.fixed_schedule_part, current_time)
        ## TODO: Urgent! make a check for abandoning of presence of duplicated tasks with state Finished, unstarted, executing
        duplicated_check = Utility.check_duplicated_tasks(resulted_schedule)

        if fpart_check is False:
            raise Exception("check for consistency with fixed schedule didn't pass")
        else:
            print("Time: " + str(current_time) + " fpart_check passed")
        if duplicated_check is False:
            raise Exception("check for duplicated tasks didn't pass")
        else:
            print("Time: " + str(current_time) + " duplicated_check passed")

        return result

    def stop(self):
        pass

    def is_running(self):
        return False
