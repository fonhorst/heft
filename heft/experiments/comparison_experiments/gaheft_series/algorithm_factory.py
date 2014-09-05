from copy import deepcopy
from functools import partial

from deap import tools
from deap.base import Toolbox
from heft.algs.common.individuals import DictBasedIndividual, FitnessStd

from heft.algs.ga.common_fixed_schedule_schema import run_ga
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.pso.sdpso import update as mapping_update


## TODO: remake interface later
## do NOT use it for anything except 'gaheft_series' experiments
from heft.algs.pso.ompso import generate, ordering_update
from heft.algs.pso.sdpso import run_pso
from heft.experiments.comparison_experiments.gaheft_series.utilities import ParticleScheduleBuilder


def create_pfga(wf, rm, estimator,
                init_sched_percent=0.05,
                **params):
    def alg(fixed_schedule_part, initial_schedule, current_time=0.0):
        def generate(n):

            init_ind_count = int(n*init_sched_percent)
            res = []
            if initial_schedule is not None and init_ind_count > 0:
                init_chromosome = DictBasedIndividual(GAFunctions2.schedule_to_chromosome(initial_schedule))
                init_arr = [deepcopy(init_chromosome) for _ in range(init_ind_count)]
                res = res + init_arr
            if n - init_ind_count > 0:
                generated_arr = [DictBasedIndividual(ga_functions.random_chromo(fixed_schedule_part, current_time))
                                 for _ in range(n - init_ind_count)]
                res = res + generated_arr
            return res

        def fit_converter(func):
            def wrap(*args, **kwargs):
                x = func(*args, **kwargs)
                return FitnessStd(values=(1/x[0], 0.0))
            return wrap

        ga_functions = GAFunctions2(wf, rm, estimator)

        toolbox = Toolbox()
        toolbox.register("generate", generate)
        toolbox.register("evaluate", fit_converter(ga_functions.build_fitness(fixed_schedule_part, current_time)))
        toolbox.register("clone", deepcopy)
        toolbox.register("mate", ga_functions.crossover)
        toolbox.register("sweep_mutation", ga_functions.sweep_mutation)
        toolbox.register("mutate", ga_functions.mutation)
        # toolbox.register("select_parents", )
        toolbox.register("select", tools.selRoulette)
        pop, logbook, best = run_ga(toolbox=toolbox, **params)

        resulted_schedule = ga_functions.build_schedule(best, fixed_schedule_part, current_time)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg


## TODO: remake interface later
## do NOT use it for anything except 'gaheft_series' experiments
def create_pfpso(wf, rm, estimator,
                init_sched_percent=0.05,
                **params):
    def alg(fixed_schedule_part, initial_schedule, current_time=0.0):

        def generate_(n):
            init_ind_count = int(n*init_sched_percent)
            res = []
            if initial_schedule is not None and init_ind_count > 0:
                heft_particle = generate(wf, rm, estimator, initial_schedule)
                init_arr = [deepcopy(heft_particle) for _ in range(init_ind_count)]
                res = res + init_arr
            if n - init_ind_count > 0:
                generated_arr = [generate(wf, rm, estimator,
                                          schedule=None,
                                          fixed_schedule_part=fixed_schedule_part,
                                          current_time=current_time)
                                 for _ in range(n - init_ind_count)]
                res = res + generated_arr
            return res

        def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
            mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
            ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)

        task_map = {task.id: task for task in wf.get_all_unique_tasks()}
        node_map = {node.name: node for node in rm.get_nodes()}

        schedule_builder = ParticleScheduleBuilder(wf, rm, estimator,
                                                   task_map, node_map,
                                                   fixed_schedule_part)
        pf_fitness = partial(schedule_builder, current_time=current_time)

        toolbox = Toolbox()
        toolbox.register("population", generate_)
        toolbox.register("fitness", pf_fitness)
        toolbox.register("update", componoud_update)

        pop, logbook, best = run_pso(toolbox=toolbox, **params)

        resulted_schedule = pf_fitness(best)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg

