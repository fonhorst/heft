from copy import deepcopy

from deap import tools
from deap.base import Toolbox

from heft.algs.ga.common_fixed_schedule_schema import run_ga
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2


## TODO: remake interface later
def create_pfga(wf, rm, estimator,
                init_sched_percent=0.05,
                **params):
    def alg(fixed_schedule_part, initial_schedule, current_time):
        def generate(n):
            init_ind_count = int(n*init_sched_percent)
            res = []
            if init_ind_count > 0:
                init_chromosome = GAFunctions2.schedule_to_chromosome(initial_schedule)
                init_arr = [deepcopy(init_chromosome) for _ in range(init_ind_count)]
                res = res + init_arr
            if n - init_ind_count > 0:
                generated_arr = [ga_functions.random_chromo(fixed_schedule_part, current_time) for _ in range(n - init_ind_count)]
                res = res + generated_arr
            return res
        ga_functions = GAFunctions2(wf, rm, estimator)
        toolbox = Toolbox()
        toolbox.register("generate", generate)
        toolbox.register("evaluate", ga_functions.build_fitness(fixed_schedule_part, current_time))
        toolbox.register("clone", deepcopy)
        toolbox.register("mate", ga_functions.crossover)
        toolbox.register("sweep_mutation", ga_functions.sweep_mutation)
        toolbox.register("mutate", ga_functions.mutation)
        # toolbox.register("select_parents", )
        toolbox.register("select", tools.selRoulette)
        result = run_ga(toolbox=toolbox, **params)
        return result
    return alg

