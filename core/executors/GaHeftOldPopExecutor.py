from collections import deque
import random
from core.CommonComponents.failers.FailOnce import FailOnce
from core.executors.EventMachine import NodeFailed, NodeUp, TaskFinished
from core.executors.GaHeftExecutor import GaHeftExecutor
from environment.ResourceManager import ScheduleItem, Schedule
from environment.Utility import Utility


class GaHeftOldPopExecutor(FailOnce, GaHeftExecutor):
    def __init__(self,
                 heft_planner,
                 base_fail_duration,
                 base_fail_dispersion,
                 fixed_interval_for_ga,
                 wf_name,
                 task_id_to_fail,
                 ga_params,
                 logger=None,
                 stat_saver=None):
        super().__init__(heft_planner,
                          base_fail_duration,
                          base_fail_dispersion,
                          fixed_interval_for_ga,
                          logger)

        self.past_pop = None
        self.failed_once = False

        self.task_id_to_fail = task_id_to_fail
        self.estimator = heft_planner.estimator

        self.ga_computation_manager = ExtendedComputationManager(fixed_interval_for_ga,
                                                                 heft_planner.workflow,
                                                                 heft_planner.resource_manager,
                                                                 heft_planner.estimator,
                                                                 wf_name,
                                                                 ga_params,
                                                                 stat_saver)

        pass

    def init(self):

        ## TODO: refactor this and base class too
        self.current_schedule = Schedule({node: [] for node in self.heft_planner.get_nodes()})
        result = self.ga_computation_manager._get_ga_alg()(self.current_schedule, None)
        self.current_schedule = result[0][2]

        self._post_new_events()

        self.ga_computation_manager.past_pop = result[0][1]
        pass

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

            # event_nodeup = NodeUp(node)
            # event_nodeup.time_happened = time_of_fail + duration

            self.post(event_failed)
            # self.post(event_nodeup)

            # remove TaskFinished event
            ##TODO: make a function for this purpose in the base class
            self.queue = deque([ev for ev in self.queue if not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id)])
        pass

    def _node_failed_handler(self, event):
        self.ga_computation_manager.current_event = event
        super()._node_failed_handler(event)
        pass

    def _node_up_handler(self, event):
        self.ga_computation_manager.current_event = event
        super()._node_up_handler(event)
        pass

    pass

class ExtendedComputationManager(object):
    def __init__(self,
                 fixed_interval_for_ga,
                 workflow,
                 resource_manager,
                 estimator,
                 wf_name,
                 ga_params,
                 stat_saver=None):
        super().__init__(fixed_interval_for_ga, workflow, resource_manager, estimator, ga_params)

        self.wf_name = wf_name
        self.past_pop = None
        self.current_event = None
        self.stat_saver = stat_saver
        pass

    def _get_result_until_current_time(self, current_time):
        ga_calc = self.current_computation[1]
        ## TODO: blocking call with complete calculation
        time_interval = current_time - ga_calc.creation_time


         ##=====================================
        ##Regular GA
        ##=====================================
        print("GaHeft WITH NEW POP: ")
        ga = self._get_ga_alg()
        ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(ga_calc.fixed_schedule_part, ga_calc.initial_schedule, current_time)

        ##=====================================
        ##Old pop GA
        ##=====================================
        print("GaHeft WITH OLD POP: ")
        cleaned_schedule = ga_calc.fixed_schedule_part
        initial_pop = [self._clean_chromosome(ind, self.current_event, cleaned_schedule) for ind in self.past_pop]
        ## TODO: rethink this hack
        result = ga_calc.run(time_interval, current_time, initial_population=initial_pop)
        (best_op, pop_op, schedule_op, stopped_iteration_op) = ga_calc.ga.get_result()
        logbook_op = ga_calc.logbook
        self.past_pop = pop_op

        ##=====================================
        ##Save stat to stat_saver
        ##=====================================
        ## TODO: make exception here
        task_id = "" if not hasattr(self.current_event, 'task') else " " + str(self.current_event.task.id)
        if self.stat_saver is not None:
            ## TODO: correct pop_agr later
            stat_data = {
                "wf_name": self.wf_name,
                "event_name": self.current_event.__class__.__name__,
                "task_id": task_id,
                "with_old_pop": {
                    "iter": stopped_iteration_op,
                    "makespan": Utility.makespan(schedule_op),
                    "pop_aggr": logbook_op
                },
                "with_random": {
                    "iter": stopped_iteration_r,
                    "makespan": Utility.makespan(schedule_r),
                    "pop_aggr": logbook_r
                }
            }
            self.stat_saver(stat_data)



        self._clean_current_computation()
        return result

    ## TODO: merge with GAOldPopExecutor
    def _clean_chromosome(self, chromosome, event, current_cleaned_schedule):

        not_scheduled_tasks = [item.job.id for (node, items) in current_cleaned_schedule.mapping.items() for item in items if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.UNSTARTED]

        for (node_name, ids) in chromosome.items():
            for_removing = []
            for id in ids:
                if id in not_scheduled_tasks:
                    for_removing.append(id)
                pass
            for r in for_removing:
                ids.remove(r)
                pass
            pass

        if isinstance(event, NodeFailed):
            tasks = chromosome[event.node.name]
            ## TODO: here must be a procedure of getting currently alive nodes
            working_nodes = list(chromosome.keys() - set([event.node.name]))
            for t in tasks:
                lt = len(working_nodes) - 1
                new_node = 0 if lt == 0 else random.randint(0, lt )
                node_name = working_nodes[new_node]
                length = len(chromosome[node_name])
                # TODO: correct 0 and length
                new_place = 0 if length == 0 else random.randint(0, length)
                chromosome[node_name].insert(new_place, t)
            chromosome[event.node.name] = []
            return chromosome
        elif isinstance(event, NodeUp):
            return chromosome
        else:
            raise Exception("Unhandled event: {0}".format(event))
        pass

    pass
