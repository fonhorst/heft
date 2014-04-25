import random
from core.CommonComponents.failers.FailOnce import FailOnce
from core.executors.EventMachine import NodeFailed, NodeUp, TaskFinished
from core.executors.GaHeftExecutor import GaHeftExecutor
from environment.ResourceManager import ScheduleItem
from environment.Utility import Utility


class GaHeftOldPopExecutor(FailOnce, GaHeftExecutor):
    def __init__(self,
                 heft_planner,
                 workflow,
                 resource_manager,
                 estimator,
                 base_fail_duration,
                 base_fail_dispersion,
                 fixed_interval_for_ga,
                 task_id_to_fail,
                 ga_builder,
                 stat_saver):
        super().__init__(heft_planner,
                          workflow,
                          resource_manager,
                          base_fail_duration,
                          base_fail_dispersion,
                          fixed_interval_for_ga,
                          ga_builder)

        self.past_pop = None
        ## TODO: move to constructor of FailOnce
        self.failed_once = False
        self.task_id_to_fail = task_id_to_fail

        self.estimator = estimator

        self.stat_saver = stat_saver

        pass

    def init(self):
        result = super().init()
        self.past_pop = result[0][1]
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

        ## TODO: may be it should be moved to specific mixin?
        if self._check_fail(event.task, node):
            # generate fail time, post it
            duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
            time_of_fail = (item.end_time - self.current_time)*random.random()
            time_of_fail = self.current_time + (time_of_fail if time_of_fail > 0 else 0.01) ##(item.end_time - self.current_time)*0.01

            event_failed = NodeFailed(node, event.task)
            event_failed.time_happened = time_of_fail

            self.post(event_failed)

            # remove TaskFinished event
            ##TODO: make a function for this purpose in the base class
            self._remove_events(lambda ev: not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id))
        pass

    def _node_failed_handler(self, event):
        self.current_event = event
        super()._node_failed_handler(event)
        pass

    def _node_up_handler(self, event):
        self.current_event = event
        super()._node_up_handler(event)
        pass

    def _actual_ga_run(self):

        ##=====================================
        ##Regular GA
        ##=====================================
        print("GaHeft WITH NEW POP: ")
        ga = self.ga_builder()
        ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(self.back_cmp.fixed_schedule,
                                                                           self.back_cmp.initial_schedule,
                                                                           self.current_time)
        ##=====================================
        ##Old pop GA
        ##=====================================
        print("GaHeft WITH OLD POP: ")
        cleaned_schedule = self.back_cmp.fixed_schedule
        initial_pop = [self._clean_chromosome(ind, self.current_event, cleaned_schedule) for ind in self.past_pop]
        ## TODO: rethink this hack
        result = self.ga_builder()(self.back_cmp.fixed_schedule,
                                   self.back_cmp.initial_schedule,
                                   self.current_time,
                                   initial_population=initial_pop)

        ((best_op, pop_op, schedule_op, stopped_iteration_op), logbook_op) = result

        self.past_pop = pop_op

        ##=====================================
        ##Save stat to stat_saver
        ##=====================================
        ## TODO: make exception here
        task_id = "" if not hasattr(self.current_event, 'task') else " " + str(self.current_event.task.id)
        if self.stat_saver is not None:
            ## TODO: correct pop_agr later
            stat_data = {
                "wf_name": self.workflow.name,
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



        self._stop_ga()
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
