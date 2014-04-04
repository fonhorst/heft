from functools import partial
import random
from core.executors.EventMachine import NodeFailed, NodeUp
from core.executors.GaHeftExecutor import GaHeftExecutor, GAComputationManager, GA_PARAMS
from environment.ResourceManager import ScheduleItem
from environment.Utility import Utility


class GaHeftOldPopExecutor(GaHeftExecutor):
    def __init__(self,
                 heft_planner,
                 base_fail_duration,
                 base_fail_dispersion,
                 fixed_interval_for_ga,
                 logger=None,
                 stat_saver=None):
        super().__init__(heft_planner,
                          base_fail_duration,
                          base_fail_dispersion,
                          fixed_interval_for_ga,
                          logger)
        self.stat_saver = None

        self.past_pop = None
        self.failed_once = False

        self.ga_computation_manager =

        pass

    def _node_failed_handler(self, event):
        super()._node_failed_handler(event)
        self.ga_computation_manager.current_event = event
        pass

    def _node_up_handler(self, event):
        super()._node_up_handler(event)
        self.ga_computation_manager.current_event = event
        pass

    pass

class ExtendedComputationManager(GAComputationManager):
    def __init__(self,
                 fixed_interval_for_ga,
                 workflow,
                 resource_manager,
                 estimator,
                 wf_name,
                 ga_params=GA_PARAMS,
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
        ga = self._get_ga_alg()
        ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(ga_calc.fixed_schedule_part, ga.initial_schedule, current_time)

        ##=====================================
        ##Old pop GA
        ##=====================================
        cleaned_schedule = ga_calc.fixed_schedule_part
        initial_pop = [self._clean_chromosome(ind, None, cleaned_schedule) for ind in self.past_pop]
        ## TODO: rethink this hack
        ga_calc.ga = partial(ga_calc.ga, initial_population=initial_pop)
        result = ga_calc.run(time_interval, current_time)
        (best_op, pop_op, schedule_op, stopped_iteration_op) = ga_calc.ga.get_result()
        self.past_pop = pop_op

        ##=====================================
        ##Save stat to stat_saver
        ##=====================================
        ## TODO: make exception here
        task_id = self.current_event.task.id
        if self.stat_saver is not None:
            stat_data = {
                "wf_name": self.wf_name,
                "event_name": self.current_event.__class__.__name__,
                "task_id": task_id,
                "with_old_pop": {
                    "iter": stopped_iteration_op,
                    "makespan": Utility.get_the_last_time(schedule_op),
                    "pop_aggr": logbook_old_pop
                },
                "with_random": {
                    "iter": stopped_iteration_r,
                    "makespan": Utility.get_the_last_time(schedule_r),
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

        def redistribute_tasks_from_failed_node():
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

        if event is None:
            return redistribute_tasks_from_failed_node()
        if isinstance(event, NodeFailed):
            return redistribute_tasks_from_failed_node()
        if isinstance(event, NodeUp):
            pass
        return chromosome

    pass
