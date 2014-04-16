from collections import deque
from copy import deepcopy
import random
from deap import creator, tools
from GA.DEAPGA.GAImplementation.GAFunctions2 import GAFunctions2
from GA.DEAPGA.GAImplementation.GAImpl import GAFactory
from GA.DEAPGA.multipopGA.MPGA import create_mpga
from core.executors.EventMachine import NodeFailed, TaskFinished
from core.executors.GaHeftExecutor import GA_PARAMS, GAComputationWrapper, SimpleGAComputationWrapper
from core.executors.GaHeftOldPopExecutor import GaHeftOldPopExecutor, ExtendedComputationManager
from environment.ResourceManager import Schedule, ScheduleItem
from environment.Utility import Utility


class MPGaHeftOldPopExecutor(GaHeftOldPopExecutor):
    def __init__(self,
                 heft_planner,
                 base_fail_duration,
                 base_fail_dispersion,
                 fixed_interval_for_ga,
                 wf_name,
                 task_id_to_fail,
                 migrCount,
                 emigrant_selection,
                 all_iters_count,
                 mixed_init_pop=False,
                 merged_pop_iters=0,
                 check_evolution_for_stopping=False,
                 ga_params=GA_PARAMS,
                 logger=None,
                 stat_saver=None):
        super().__init__(heft_planner,
                         base_fail_duration,
                         base_fail_dispersion,
                         fixed_interval_for_ga,
                         wf_name,
                         task_id_to_fail,
                         ga_params,
                         logger,
                         stat_saver)

        self.ga_computation_manager = MPCm(fixed_interval_for_ga,
                                                                 heft_planner.workflow,
                                                                 heft_planner.resource_manager,
                                                                 heft_planner.estimator,
                                                                 wf_name,
                                                                 migrCount,
                                                                 emigrant_selection,
                                                                 all_iters_count,
                                                                 mixed_init_pop,
                                                                 merged_pop_iters,
                                                                 check_evolution_for_stopping,
                                                                 ga_params,
                                                                 stat_saver)
        pass

    def init(self):
        ## TODO: refactor this and base class too
        self.current_schedule = Schedule({node: [] for node in self.heft_planner.get_nodes()})
        result = self.ga_computation_manager.run_init_ga(self.current_schedule)

        self.current_schedule = result[0][2]

        self._post_new_events()

        self.ga_computation_manager.past_pop = result[0][1]



    def _check_event_for_ga_result(self, event):
         # check for time to get result from GA running background
        result = self.ga_computation_manager.check_result(event, self.current_time)
        if result is not None:
            resulted_schedule = result
            t1 = Utility.get_the_last_time(resulted_schedule)
            t2 = Utility.get_the_last_time(self.current_schedule)
            ## TODO: uncomment it later.
            ## TODO: we don't care about quality of result
            ## generate new events
            self._replace_current_schedule(event, resulted_schedule)
            ## if event is TaskStarted event the return value means skip further processing
            return True
        return False

## TODO: replace all this params with context
class MPCm(ExtendedComputationManager):
    def __init__(self,
                   fixed_interval_for_ga,
                   workflow,
                   resource_manager,
                   estimator,
                   wf_name,
                   migrCount,
                   emigrant_selection,
                   all_iters_count,
                   mixed_init_pop,
                   merged_pop_iters,
                   check_evolution_for_stopping,
                   ga_params=GA_PARAMS,
                   stat_saver=None):
         super().__init__(fixed_interval_for_ga,
                        workflow,
                        resource_manager,
                        estimator,
                        wf_name,
                        ga_params,
                        stat_saver)

         self.migrCount = migrCount
         self.emigrant_selection = emigrant_selection
         self.all_iters_count = all_iters_count
         self.mixed_init_pop = mixed_init_pop
         self.merged_pop_iters = merged_pop_iters
         self.check_evolution_for_stopping = check_evolution_for_stopping
         pass

    def _get_ga_alg(self):
         ga = create_mpga(silent=True,
                             wf=self.workflow,
                             resource_manager=self.resource_manager,
                             estimator=self.estimator,
                             ga_params=self.params,
                             migrCount=self.migrCount,
                             emigrant_selection=self.emigrant_selection,
                             all_iters_count=self.all_iters_count,
                             merged_pop_iters=self.merged_pop_iters,
                             check_evolution_for_stopping=self.check_evolution_for_stopping
                             )

         return ga

    def run_init_ga(self, init_schedule):

         # ga = GAFactory.default().create_ga(silent=True,
         #                                         wf=self.workflow,
         #                                         resource_manager=self.resource_manager,
         #                                         estimator=self.estimator,
         #                                         ga_params=self.params)

         ga = self._get_simple_ga()

         result = ga(init_schedule, None)
         return result

    def _create_comp_wrapper(self, ga, fixed_schedule, current_schedule, current_time):
        heft_initial = GAFunctions2.schedule_to_chromosome(current_schedule)
        heft_initial = self._clean_chromosome(heft_initial, self.current_event, fixed_schedule)

        ## TODO: remake this stub. GAComputationWrapper must take cleaned current_schedule
        #return GAComputationWrapper(ga, fixed_schedule, heft_initial, current_time)
        return SimpleGAComputationWrapper(ga, fixed_schedule, heft_initial, current_time)

    def _get_simple_ga(self):
        ga = GAFactory.default().create_ga(silent=True,
                                                 wf=self.workflow,
                                                 resource_manager=self.resource_manager,
                                                 estimator=self.estimator,
                                                 ga_params={
                                        ## TODO: add a param to describe it
                                        ## 3 is populations count
                                        "population": self.params["population"] * 3,
                                        "crossover_probability": self.params["crossover_probability"],
                                        "replacing_mutation_probability": self.params["replacing_mutation_probability"],
                                        "sweep_mutation_probability": self.params["sweep_mutation_probability"],
                                        "generations": self.all_iters_count
                                     })

        return ga


    def _get_result_until_current_time(self, current_time):
        ga_calc = self.current_computation[1]
        ## TODO: blocking call with complete calculation
        time_interval = current_time - ga_calc.creation_time


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
            heft_initial = ga_calc.initial_schedule
            heft_initial = tools.initIterate(creator.Individual, lambda: heft_initial)
            ga_functions = GAFunctions2(self.workflow, self.resource_manager, self.estimator)
            heft_pop = [ga_functions.mutation(deepcopy(heft_initial)) for i in range(self.params["population"])]
            cleaned_schedule = ga_calc.fixed_schedule_part
            initial_pop_gaheft = [self._clean_chromosome(deepcopy(p), self.current_event, cleaned_schedule) for p in self.past_pop] + heft_pop

        print("GaHeft WITH NEW POP: ")
        ga = self._get_simple_ga()
        ((best_r, pop_r, schedule_r, stopped_iteration_r), logbook_r) = ga(ga_calc.fixed_schedule_part, None, current_time, initial_population=initial_pop_gaheft)



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
        task_id = "" if not hasattr(self.current_event, 'task') else str(self.current_event.task.id)
        if self.stat_saver is not None:
            ## TODO: correct pop_agr later
            stat_data = {
                "wf_name": self.wf_name,
                "event_name": self.current_event.__class__.__name__,
                "task_id": task_id,
                "with_old_pop": {
                    "iter": stopped_iteration_op,
                    "makespan": Utility.get_the_last_time(schedule_op),
                    "pop_aggr": logbook_op
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