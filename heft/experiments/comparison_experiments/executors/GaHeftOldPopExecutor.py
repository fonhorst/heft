from copy import deepcopy
import random

from heft.core.CommonComponents.failers.FailOnce import FailOnce
from heft.core.environment.Utility import Utility
from heft.core.environment.EventMachine import NodeFailed, NodeUp, TaskFinished
from heft.experiments.comparison_experiments.executors.GaHeftExecutor import GaHeftExecutor
from heft.core.environment.ResourceManager import ScheduleItem
from heft.experiments.comparison_experiments.executors.InheritedGaHeftExecutor import InheritedGaHeftExecutor
from heft.utilities.common import trace


class GaHeftOldPopExecutor(FailOnce, InheritedGaHeftExecutor):
    #@trace
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.past_pop = None
        ## TODO: move to constructor of FailOnce
        self.failed_once = False

        self.task_id_to_fail = kwargs["task_id_to_fail"]
        self.estimator = kwargs["estimator"]
        self.stat_saver = kwargs.get("stat_saver", None)
        self.chromosome_cleaner = kwargs["chromosome_cleaner"]
        self.replace_anyway = True

        self.executor_stat_data = None

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
            #self._remove_events(lambda ev: not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id))

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

        # initial_pop = [self._clean_chromosome(ind, self.current_event, cleaned_schedule) for ind in self.past_pop]
        backup_copy = deepcopy(self.past_pop)
        initial_pop = [self._clean_chromosome(ind, self.current_event, cleaned_schedule) for ind in self.past_pop]
        backup_copy_init_pop = deepcopy(initial_pop)
        ## TODO: rethink this hack
        result = self.ga_builder()(self.back_cmp.fixed_schedule,
                                   self.back_cmp.initial_schedule,
                                   self.current_time,
                                   initial_population=initial_pop)
                                   # initial_population=None)

        ((best_op, pop_op, schedule_op, stopped_iteration_op), logbook_op) = result

        self.past_pop = pop_op

        ## save logbook data
        self.executor_stat_data = {
            "event": str(self.current_event),
            "random_init_logbook": logbook_r,
            "inherited_init_logbook": logbook_op

        }

        #
        # ## TODO: obsolete. remove it later
        # ##=====================================
        # ##Save stat to stat_saver
        # ##=====================================
        # ## TODO: make exception here
        # task_id = "" if not hasattr(self.current_event, 'task') else str(self.current_event.task.id)
        # if self.stat_saver is not None:
        #     ## TODO: correct pop_agr later
        #     stat_data = {
        #         "wf_name": self.workflow.name,
        #         "event_name": self.current_event.__class__.__name__,
        #         "task_id": task_id,
        #         "with_old_pop": {
        #             "iter": stopped_iteration_op,
        #             "makespan": Utility.makespan(schedule_op),
        #             "pop_aggr": logbook_op
        #         },
        #         "with_random": {
        #             "iter": stopped_iteration_r,
        #             "makespan": Utility.makespan(schedule_r),
        #             "pop_aggr": logbook_r
        #         }
        #     }
        #     self.stat_saver(stat_data)
        #
        # #TODO: remove this hack later
        # self.stop()

        self._stop_ga()
        return result

    ## TODO: merge with GAOldPopExecutor
    #@timing
    def _clean_chromosome(self, chromosome, event, current_cleaned_schedule):
        print("Current event {0}".format(event))
        if isinstance(event, NodeFailed):
            return self.chromosome_cleaner(chromosome, current_cleaned_schedule)
        elif isinstance(event, NodeUp):
            return chromosome
            #return self.chromosome_cleaner(chromosome, current_cleaned_schedule)
        else:
            raise Exception("Unhandled event: {0}".format(event))
        pass



    pass
