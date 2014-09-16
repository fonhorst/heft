from copy import deepcopy
from heft.core.CommonComponents.failers.FailRandom import FailRandom
from heft.experiments.comparison_experiments.executors.GaHeftOldPopExecutor import GaHeftOldPopExecutor
from heft.utilities.common import trace
import inspect



class MIGaHeftExecutor(GaHeftOldPopExecutor):
    #@trace
    def __init__(self, *args, **kwargs):
        #kwargs = deepcopy(kwargs)
        #kwargs["replace_anyway"] = True
        super().__init__(**kwargs)
        self.schedule_to_chromosome_converter = kwargs["schedule_to_chromosome_converter"]

        #self.replace_anyway = True
        pass

    def _actual_ga_run(self):
        ## TODO: correct this.
        heft_initial = self.schedule_to_chromosome_converter(self.back_cmp.current_schedule)
        heft_initial = self._clean_chromosome(heft_initial, self.back_cmp.event, self.back_cmp.fixed_schedule)


        cleaned_schedule = self.back_cmp.fixed_schedule
        initial_pop = [self._clean_chromosome(ind, self.back_cmp.event, cleaned_schedule) for ind in self.past_pop]


        result = self.ga_builder()(self.back_cmp.fixed_schedule,
                                     heft_initial,
                                     self.current_time,
                                     initial_population=initial_pop)
        ((best_op, pop_op, schedule_op, stopped_iteration_op), logbook_op) = result

        self.past_pop = pop_op

        ## save logbook data
        self.executor_stat_data = {
            "event": str(self.current_event),
            "inherited_init_logbook": logbook_op

        }

        #self._stop_ga()
        return result

    def _check_fail(self, task, node):
        return FailRandom._check_fail(self, task, node)
    pass

## TODO: remove it later.
# if __name__ == "__main__":
#     kwargs = {
#         "mpga_builder": None,
#         "schedule_to_chromosome_converter": None,
#         "task_id_to_fail": None,
#         "estimator": None,
#         "stat_saver": None,
#         "chromosome_cleaner": None,
#         "wf": None,
#         "resource_manager": None,
#         # DynamicHeft
#         # both planners have acess to resource manager and estimator
#         "heft_planner": None,
#         "base_fail_duration": None,
#         "base_fail_dispersion": None,
#         "fixed_interval_for_ga": None,
#         "ga_builder": None,
#
#     }
#
#     result = inspect.getmro(MIGaHeftExecutor)
#     print(result)
#
#     obj = MIGaHeftExecutor(**kwargs)
#     obj._check_fail(None, None)