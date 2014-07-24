from copy import deepcopy

from deap import creator, tools
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.core.environment.Utility import Utility
from heft.experiments.comparison_experiments.executors.GaHeftOldPopExecutor import GaHeftOldPopExecutor


class MPGaHeftOldPopExecutor(GaHeftOldPopExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.mpga_builder = kwargs["mpga_builder"]

        self.migrCount = kwargs["migrCount"]
        self.emigrant_selection = kwargs["emigrant_selection"]
        self.all_iters_count = kwargs["all_iters_count"]
        self.mixed_init_pop = kwargs["mixed_init_pop"]
        self.merged_pop_iters = kwargs["merged_pop_iters"]
        self.check_evolution_for_stopping = kwargs["check_evolution_for_stopping"]
        self.mpnewVSmpoldmode = kwargs["mpnewVSmpoldmode"]
        self.params = kwargs["ga_params"]

        self.replace_anyway = True
        # self.ga_computation_manager = MPCm(**kwargs)
        pass

    def _actual_ga_run(self):
        heft_initial = GAFunctions2.schedule_to_chromosome(self.back_cmp.current_schedule)
        heft_initial = self._clean_chromosome(heft_initial, self.back_cmp.event, self.back_cmp.fixed_schedule)

         ##=====================================
        ##Regular GA
        ##=====================================
        # print("GaHeft WITH NEW POP: ")
        # ga = self._get_ga_alg()
        # ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(ga_calc.fixed_schedule_part, ga_calc.initial_schedule, current_time)

        ## TODO: make here using of param to decide how to build initial population for regular gaheft
        ## TODO: self.mixed_init_pop doesn't exist and isn't setted anywhere
        ## TODO: need to refactor and merge with MPGA.py
        initial_pop_gaheft = None
        if self.mixed_init_pop is True:
            heft_initial = tools.initIterate(creator.Individual, lambda: heft_initial)
            ga_functions = GAFunctions2(self.workflow, self.resource_manager, self.estimator)
            heft_pop = [ga_functions.mutation(deepcopy(heft_initial)) for i in range(self.params["population"])]
            cleaned_schedule = self.back_cmp.fixed_schedule
            initial_pop_gaheft = [self._clean_chromosome(deepcopy(p), self.back_cmp.event, cleaned_schedule) for p in self.past_pop] + heft_pop


        print("GaHeft WITH NEW POP: ")
        if self.mpnewVSmpoldmode is True:
            print("using multi population gaheft...")
            ga = self.mpga_builder()
            ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(self.back_cmp.fixed_schedule, None, self.current_time, initial_population=None, only_new_pops=True)
        else:
            print("using single population gaheft...")
            ga = self.ga_builder()
            ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(self.back_cmp.fixed_schedule, None, self.current_time, initial_population=initial_pop_gaheft)



        ##=====================================
        ##Old pop GA
        ##=====================================
        print("GaHeft WITH OLD POP: ")
        cleaned_schedule = self.back_cmp.fixed_schedule
        initial_pop = [self._clean_chromosome(ind, self.back_cmp.event, cleaned_schedule) for ind in self.past_pop]


        result = self.mpga_builder()(self.back_cmp.fixed_schedule,
                                     heft_initial,
                                     self.current_time,
                                     initial_population=initial_pop)
        ((best_op, pop_op, schedule_op, stopped_iteration_op), logbook_op) = result

        self.past_pop = pop_op

        ##=====================================
        ##Save stat to stat_saver
        ##=====================================
        ## TODO: make exception here
        task_id = "" if not hasattr(self.current_event, 'task') else str(self.current_event.task.id)
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