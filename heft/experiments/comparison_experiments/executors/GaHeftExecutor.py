from collections import namedtuple
from copy import deepcopy
import random

from heft.core.CommonComponents.BaseExecutor import BaseExecutor
from heft.core.CommonComponents.failers.FailRandom import FailRandom
from heft.core.environment.Utility import Utility
from heft.core.environment.EventMachine import TaskFinished, NodeFailed, NodeUp
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import ScheduleItem, Schedule


BackCmp = namedtuple('BackCmp', ['fixed_schedule', 'initial_schedule', 'current_schedule', 'event', 'creation_time', 'time_to_stop'])

class GaHeftExecutor(FailRandom, BaseExecutor):
    def __init__(self, **kwargs):

        super().__init__()

        self.workflow = kwargs["wf"]
        self.resource_manager = kwargs["resource_manager"]
        # DynamicHeft
        # both planners have acess to resource manager and estimator
        self.heft_planner = kwargs["heft_planner"]
        self.base_fail_duration = kwargs["base_fail_duration"]
        self.base_fail_dispersion = kwargs["base_fail_dispersion"]
        self.current_schedule = None
        self.fixed_interval_for_ga = kwargs["fixed_interval_for_ga"]
        self.ga_builder = kwargs["ga_builder"]
        self.replace_anyway = kwargs.get("replace_anyway", False)

        self.back_cmp = None

        pass

    def init(self):
        self.current_schedule = Schedule({node: [] for node in self.heft_planner.get_nodes()})

        initial_schedule = self.heft_planner.run(deepcopy(self.current_schedule))
        #print("HEFT MAKESPAN: {0}".format(Utility.makespan(initial_schedule)))
        # TODO: change these two ugly records
        result = self.ga_builder()(self.current_schedule, initial_schedule)
        #print("INIT MAKESPAN: {0}".format(Utility.makespan(result[0][2])))
        self.current_schedule = result[0][2]

        self._post_new_events()
        return result

    def _task_start_handler(self, event):

        res = self._check_event_for_ga_result(event)
        if res:
            return
        # check task as executing
        # self.current_schedule.change_state(event.task, ScheduleItem.EXECUTING)
        # try to find nodes in cloud
        # check if failed and post
        (node, item) = self.current_schedule.place_by_time(event.task, event.time_happened)
        item.state = ScheduleItem.EXECUTING

        if self._check_fail(event.task, node):
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

            self._remove_events(lambda ev: not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id))
        pass

    def _task_finished_handler(self, event):
        # check task finished
        self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)
        self._check_event_for_ga_result(event)
        pass

    def _node_failed_handler(self, event):
        ## interrupt ga
        self._stop_ga()
        # check node down
        self.resource_manager.node(event.node).state = Node.Down
        # check failed event in schedule
        ## TODO: ambigious choice
        ##self.current_schedule.change_state(event.task, ScheduleItem.FAILED)
        it = [item for item in self.current_schedule.mapping[event.node] if item.job.id == event.task.id and item.state == ScheduleItem.EXECUTING]
        if len(it) != 1:
            ## TODO: raise exception here
            raise Exception(" Trouble in finding of the task: count of found tasks {0}".format(len(it)))

        it[0].state = ScheduleItem.FAILED
        it[0].end_time = self.current_time

        # run HEFT
        self._reschedule(event)
        #run GA
        self._run_ga_in_background(event)
        pass

    def _node_up_handler(self, event):
        ## interrupt ga
        self._stop_ga()
        # check node up
        self.heft_planner.resource_manager.node(event.node).state = Node.Unknown
        self._reschedule(event)
        #run GA
        self._run_ga_in_background(event)
        pass

    def _stop_ga(self):
        self.back_cmp = None
        pass

    def _actual_ga_run(self):

        ## this way makes it possible to calculate what time
        ## ga actually has to find solution
        ## this value is important when you need account events between
        ## planned start and stop points
        # ga_interval = self.current_time - self.back_cmp.creation_time

        ## fixed_schedule is actual because
        ## we can be here only if there haven't been any invalidate events
        ## such as node failures
        ## in other case current ga background computation would be dropped
        ## and we wouldn't get here at all
        result = self.ga_builder()(self.back_cmp.fixed_schedule,
                                   # self.back_cmp.initial_schedule,
                                   self.back_cmp.current_schedule,
                                   self.current_time)
        print("CURRENT MAKESPAN: {0}".format(Utility.makespan(result[0][2])))
        return result

    def _check_event_for_ga_result(self, event):
        # check for time to get result from GA running background
        if self.back_cmp is None or self.back_cmp.time_to_stop != self.current_time:
            return False
        else:
            result = self._actual_ga_run()

        if result is not None:
            t1 = Utility.makespan(result[0][2])
            t2 = Utility.makespan(self.current_schedule)
            if self.replace_anyway is True or t1 < t2:
                ## generate new events
                self._replace_current_schedule(event, result[0][2])
                ## if event is TaskStarted event the return value means skip further processing
                return True
            else:
                ## TODO: run_ga_yet_another_with_old_genome
                # self.ga_computation_manager.run(self.current_schedule, self.current_time)
                self._run_ga_in_background(event)
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

    def _run_ga_in_background(self, event):
        current_schedule = self.current_schedule
        current_time = self.current_time
        ## TODO: replace by log call
        print("Time: " + str(current_time) + " Creating reschedule point ")
        ## there can be several events in one time
        ## we choose the first to handle background GA run
        def _get_front_line(schedule, current_time, fixed_interval):
            event_time = current_time + fixed_interval
            min_item = ScheduleItem.MIN_ITEM()

            for (node, items) in schedule.mapping.items():
                for item in items:
                    ## It accounts case when event_time appears in a transfer gap(rare situation for all nodes)
                    ## TODO: compare with some precison
                    if event_time < item.end_time < min_item.end_time:
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
                ## TODO: Urgent!!! experimental change. Perhaps, It should be removed from here later.
                if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.FAILED:
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

            #TODO: It isn't a good reliable solution. It should be reconsider later.
            fixed_ids = set(fixed_schedule.get_all_unique_tasks_id())
            all_ids = set(task.id for task in self.workflow.get_all_unique_tasks())

            ## TODO: urgent bugfix to correctly run GaHeftvsHeft
            if len(fixed_ids) == len(all_ids):
                print("Fixed schedule is complete. There is no use to run ga.")
                return

            self.back_cmp = BackCmp(fixed_schedule, None, self.current_schedule, event, current_time, front_event.end_time)
            pass

        is_running = self.back_cmp is not None

        if not is_running:
            run_ga(current_schedule)
        else:
            self.back_cmp = None
            run_ga(current_schedule)

    pass