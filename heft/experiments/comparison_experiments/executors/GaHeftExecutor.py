from collections import namedtuple
from copy import deepcopy, copy
import functools
import operator
import pprint
import random
from heft.core.CommonComponents import BladeExperimentalManager

from heft.core.CommonComponents.BaseExecutor import BaseExecutor
from heft.core.CommonComponents.failers.FailRandom import FailRandom
from heft.core.environment.Utility import Utility
from heft.core.environment.EventMachine import TaskFinished, NodeFailed, NodeUp, TaskStart, ResourceFailed, ResourceUp
from heft.core.environment.BaseElements import Node, Resource
from heft.core.environment.ResourceManager import ScheduleItem, Schedule
from heft.utilities.common import trace


BackCmp = namedtuple('BackCmp', ['fixed_schedule', 'initial_schedule', 'current_schedule', 'event', 'creation_time', 'time_to_stop'])

class GaHeftExecutor(FailRandom, BaseExecutor):
    #@trace
    def __init__(self, **kwargs):

        super().__init__(**kwargs)

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
        self.replace_anyway = kwargs.get("replace_anyway", True)

        self.back_cmp = None

        pass

    def init(self):
        self.current_schedule = Schedule({node: [] for node in self.heft_planner.get_nodes()})

        initial_schedule = self.heft_planner.run(Schedule({node: [] for node in self.heft_planner.get_nodes()}))

        # TODO: change these two ugly records
        result = self.ga_builder()(self.current_schedule, initial_schedule)

        if not self._apply_mh_if_better(None, heuristic_resulted_schedule=initial_schedule,
                           metaheuristic_resulted_schedule=result[0][2]):
            self.current_schedule = initial_schedule
            self._post_new_events()

        return result


    def _check_resource_fail(self, task, node):
        return False


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

        if not self._is_a_fail_possible():
            return


        if self._check_fail(event.task, node):
            # generate fail time, post it
            duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
            time_of_fail = (item.end_time - self.current_time)*random.random()
            time_of_fail = self.current_time + (time_of_fail if time_of_fail > 0 else 0.01) ##(item.end_time - self.current_time)*0.01

            ## TODO: refactor
            if self._check_resource_fail(event.task, node):
                duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
                time_of_fail = (item.end_time - self.current_time)*random.random()
                time_of_fail = self.current_time + (time_of_fail if time_of_fail > 0 else 0.01) ##(item.end_time - self.current_time)*0.01

                resource = self.resource_manager.res_by_id(node.resource)
                event_failed = ResourceFailed(resource, event.task)
                event_failed.time_happened = time_of_fail
                self.post(event_failed)

                if self.base_fail_duration != -1:
                    duration = self.base_fail_duration + self.base_fail_dispersion * random.random()
                    event_up = ResourceUp(resource, event_failed)
                    event_up.time_happened = time_of_fail + duration
                    self.post(event_up)

                return

            event_failed = NodeFailed(node, event.task)
            event_failed.time_happened = time_of_fail
            self.post(event_failed)

            if self.base_fail_duration != -1:
                duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
                event_nodeup = NodeUp(node, event_failed)
                event_nodeup.time_happened = time_of_fail + duration
                self.post(event_nodeup)

        pass

    def _task_finished_handler(self, event):
        # check task finished
        self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)
        self._check_event_for_ga_result(event)
        pass

    def _node_failed_handler(self, event):

        if not self._is_a_fail_possible():
            return

           ## TODO: refactor
        def remove_all(ev):
            # False means Remove
            # True means Save
            if isinstance(ev, TaskFinished) and ev.task.id == event.task.id: return False
            if isinstance(ev, ResourceFailed) and ev.resource.name == event.node.resource.name: return False
            # TODO: correct id
            if isinstance(ev, ResourceUp) and ev.resource.name == event.node.resource.name: return False
            return True

        self._remove_events(remove_all)

        ## interrupt ga
        self._stop_ga()
        # check node down
        self.resource_manager.node(event.node).state = Node.Down
        # check failed event in schedule
        ## TODO: ambigious choice
        ##self.current_schedule.change_state(event.task, ScheduleItem.FAILED)
        it = [item for item in self.current_schedule.mapping[event.node] if item.job.id == event.task.id and item.state == ScheduleItem.EXECUTING]
        if len(it) != 1:
            raise Exception(" Trouble in finding of the task: count of found tasks {0}".format(len(it)))

        it[0].state = ScheduleItem.FAILED
        it[0].end_time = self.current_time

        ## change for Blade Resource (Vm resources)
        if isinstance(self.resource_manager, BladeExperimentalManager.ExperimentResourceManager):
            self.resource_manager.resource(event.node.resource).farm_capacity -= event.node.flops

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

        ## change for Blade Resource (Vm resources)
        if isinstance(self.resource_manager, BladeExperimentalManager.ExperimentResourceManager):
            self.resource_manager.resource(event.node.resource).farm_capacity += event.node.flops

        self._reschedule(event)

        #run GA
        self._run_ga_in_background(event)
        pass

    def _resource_failed_handler(self, event):

        if not self._is_a_fail_possible():
            return




        # print("Debug info: ")
        # print("Failed task: ", event.task)
        # print("Nodes: ", self.resource_manager.get_nodes_by_resource(event.resource))

        nodes = self.resource_manager.get_nodes_by_resource(event.resource)

        ## check if there is any live nodes after killing of the resource,
        ## if not, don't kill the resource - cancel the killing
        live_nodes = [node for node in self.resource_manager.get_nodes()
                      if node.resource.name == event.resource.name]
        if set(nodes) == set(live_nodes):
            self._remove_events(lambda ev: not (isinstance(ev, ResourceUp) and ev.failed_event != event))
            return


        ## TODO: refactor
        def remove_all(ev):
            # False means Remove
            # True means Save
            if isinstance(ev, TaskFinished) and ev.task.id == event.task.id: return False
            if isinstance(ev, NodeFailed) and ev.node.resource.name == event.resource.name: return False
            if isinstance(ev, NodeUp) and ev.node.resource.name == event.resource.name: return False
            if isinstance(ev, ResourceFailed) and ev.resource.name == event.resource.name: return False
            if isinstance(ev, ResourceUp) and ev.failed_event != event: return False
            return True

        self._remove_events(remove_all)

        ## interrupt ga
        self._stop_ga()
        # check node down

        for node in nodes:
            node.state = Node.Down
            it = [item for item in self.current_schedule.mapping[node]
                  if item.start_time <= event.time_happened < item.end_time and item.state == ScheduleItem.EXECUTING]
            if len(it) > 0:
                it[0].state = ScheduleItem.FAILED
                it[0].end_time = self.current_time
        self.resource_manager.resource(event.resource).state = Resource.Down


        # run HEFT
        self._reschedule(event)

        #run GA
        self._run_ga_in_background(event)
        pass

    def _resource_up_handler(self, event):
        ## interrupt ga
        self._stop_ga()
        # check node up
        self.resource_manager.resource(event.resource).state = Resource.Unknown

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
            print("Event {0}".format(event))
            if isinstance(event, TaskStart):
                print("Task id {0}".format(event.task.id))
            result = self._actual_ga_run()

        if result is not None:
            return self._apply_mh_if_better(event, heuristic_resulted_schedule=self.current_schedule,
                                      metaheuristic_resulted_schedule=result[0][2])

        return False

    def _replace_current_schedule(self, event, new_schedule):
        # syncrhonize fixed part of new_schedule with the old schedule - lets assume new_schedule already synchonized
        # remove all events related with the old schedule
        # replace current with new
        # generate events of new schedule and post their
        if event is not None:
            self._clean_events(event)
        self.current_schedule = new_schedule
        self._post_new_events()

        self.back_cmp = None
        pass

    def _apply_mh_if_better(self, event, heuristic_resulted_schedule, metaheuristic_resulted_schedule):
        t1 = Utility.makespan(metaheuristic_resulted_schedule)
        t2 = Utility.makespan(heuristic_resulted_schedule)
        # print("Replace anyway - {0}".format(self.replace_anyway))
        if self.replace_anyway is True or t1 < t2:
            # print("Replacing temp schedule with a mh-generated schedule. Temp - {0}, mh - {1}".format(t2, t1))
            ## generate new events

            ##TODO: debug output
            # print("=====================================================")
            # print("============HEURO===========")
            # print("=====================================================")
            # pprint.pprint(heuristic_resulted_schedule.mapping)
            # print("=====================================================")
            # print("============MHEURO===========")
            # print("=====================================================")
            # pprint.pprint(metaheuristic_resulted_schedule.mapping)

            self._replace_current_schedule(event, metaheuristic_resulted_schedule)
            ## if event is TaskStarted event the return value means skip further processing
            return True
        else:
            ## TODO: run_ga_yet_another_with_old_genome
            # self.ga_computation_manager.run(self.current_schedule, self.current_time)
            #self._run_ga_in_background(event)
            self.back_cmp = None
            return False
        pass

    # def _is_a_fail_possible(self):
    #     live_resources_overall = [res for res in self.resource_manager.get_resources() if res.state != Resource.Down]
    #     live_nodes_overall = [nd for nd in self.resource_manager.get_nodes()if nd.state != Node.Down]
    #
    #     if len(live_nodes_overall) == 1:
    #         print("DECLINE NODE DOWN")
    #         st = functools.reduce(operator.add, (" {0} - {1}".format(nd.name, nd.state) for nd in self.resource_manager.get_nodes()), "")
    #         print("STATE INFORMATION: " + st)
    #         return False
    #     print("Live resources: ", live_resources_overall)
    #     print("Live nodes: ", live_nodes_overall)
    #     return True

    ## kill of the last alive node is not allowed
    def _is_a_fail_possible(self):
        live_nodes = [node for node in self.resource_manager.get_nodes() if node.state != Node.Down]
        if len(live_nodes) == 1:
            return False
        return True


    def _run_ga_in_background(self, event):

        if len([nd for nd in self.resource_manager.get_nodes() if nd.state != Node.Down]) == 0:
            return

        current_schedule = self.current_schedule
        current_time = self.current_time
        ## TODO: replace by log call
        print("Time: " + str(current_time) + " Creating reschedule point ")
        ## there can be several events in one time
        ## we choose the first to handle background ga run
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
                print("ga's computation isn't able to meet the end of computation")
                return
            fixed_schedule = _get_fixed_schedule(current_schedule, front_event)

            #TODO: It isn't a good reliable solution. It should be reconsider later.
            fixed_ids = set(fixed_schedule.get_all_unique_tasks_id())
            all_ids = set(task.id for task in self.workflow.get_all_unique_tasks())

            ## TODO: urgent bugfix to correctly run GaHeftvsHeft
            if len(fixed_ids) == len(all_ids):
                print("Fixed schedule is complete. There is no use to run ga.")
                return

            fsh = [hash(key) for key in fixed_schedule.mapping.keys()]
            rm_hashes = [hash(node) for node in self.resource_manager.get_nodes()]
            # if any(((h not in fsh) for h in rm_hashes)):
            #     raise Exception("Fixed schedule is broken")

            self.back_cmp = BackCmp(fixed_schedule, None, self.current_schedule, event, current_time, front_event.end_time)
            pass

        is_running = self.back_cmp is not None

        if not is_running:
            run_ga(current_schedule)
        else:
            self.back_cmp = None
            run_ga(current_schedule)


        ## TODO: only for debug. remove it later.
        # print("==================FIXED SCHEDULE PART=================")
        # print(self.back_cmp.fixed_schedule)
        # print("======================================================")

    pass