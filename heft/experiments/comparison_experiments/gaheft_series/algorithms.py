from copy import deepcopy
from functools import partial

from deap import tools
from deap.base import Toolbox

from heft.algs.common.individuals import FitnessStd
from heft.algs.ga.GAImplementation.GAImpl import GAFactory
from heft.algs.ga.common_fixed_schedule_schema import run_ga, fit_converter
from heft.algs.ga.common_fixed_schedule_schema import generate as ga_generate
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.pso.mapping_operators import update as mapping_update

from heft.algs.pso.ordering_operators import generate as pso_generate, ordering_update
from heft.algs.pso.sdpso import run_pso
from heft.core.environment.Utility import Utility
from heft.experiments.comparison_experiments.common.chromosome_cleaner import GaChromosomeCleaner, PSOChromosomeCleaner
from heft.experiments.comparison_experiments.gaheft_series.utilities import ParticleScheduleBuilder


def create_old_ga(wf, rm, estimator,
                  init_sched_percent=0.05,
                  **params):
    kwargs = {}
    kwargs["wf"] = wf
    kwargs["resource_manager"] = rm
    kwargs["estimator"] = estimator
    kwargs["ga_params"] = {
        "population": params["n"],
        "crossover_probability": params["cxpb"],
        "replacing_mutation_probability": params["mutpb"],
        "generations": params["gen_step"],
        "sweep_mutation_probability": params["sweepmutpb"],
        "Kbest": params["kbest"]
    }
    kwargs["silent"] = params["is_silent"]
    ga = partial(GAFactory.default().create_ga, **kwargs)
    return ga()


def create_pfga(wf, rm, estimator,
                init_sched_percent=0.05,
                **params):
    ##TODO: add initial_population for run_ga
    raise NotImplementedError()

    ga_functions = GAFunctions2(wf, rm, estimator)

    def alg(fixed_schedule_part, initial_schedule, current_time=0.0, initial_population=None):



        ga_functions = GAFunctions2(wf, rm, estimator)
        generate = partial(ga_generate, ga_functions=ga_functions,
                           fixed_schedule_part=fixed_schedule_part,
                           current_time=current_time, init_sched_percent=init_sched_percent,
                           initial_schedule=initial_schedule)

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

    def alg(fixed_schedule_part, initial_schedule, current_time=0.0, initial_population=None):
        def generate_(n):
            init_ind_count = int(n*init_sched_percent)
            res = []
            # init_pop_size = 0
            init_pop_size = 0 if initial_population is None else len(initial_population)
            if init_pop_size > 0:
                if init_pop_size > n:
                    raise ValueError("size of initial population is bigger than parameter n: {0} > {1}".
                                         format(init_pop_size, n))


                res = res + initial_population
            if initial_schedule is not None and init_ind_count > 0 and n - init_pop_size > 0:
                heft_particle = pso_generate(wf, rm, estimator, initial_schedule)
                init_arr = [deepcopy(heft_particle) for _ in range(init_ind_count)]
                for p in init_arr:
                    p.created_by = "heft_particle"
                res = res + init_arr
            if n - init_ind_count - init_pop_size > 0:
                generated_arr = [pso_generate(wf, rm, estimator,
                                          schedule=None,
                                          fixed_schedule_part=fixed_schedule_part,
                                          current_time=current_time)
                                 for _ in range(n - init_ind_count)]
                for p in generated_arr:
                    p.created_by = "generator"
                res = res + generated_arr
            return res

        def fit_converter(func):
            def wrap(*args, **kwargs):
                x = func(*args, **kwargs)
                m = Utility.makespan(x)
                return FitnessStd(values=(m, 0.0))
            return wrap

        def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
            mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
            ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)

        task_map = {task.id: task for task in wf.get_all_unique_tasks()}
        node_map = {node.name: node for node in rm.get_nodes()}

        schedule_builder = ParticleScheduleBuilder(wf, rm, estimator,
                                                   task_map, node_map,
                                                   fixed_schedule_part)
        pf_schedule = partial(schedule_builder, current_time=current_time)

        toolbox = Toolbox()
        toolbox.register("population", generate_)
        toolbox.register("fitness", fit_converter(pf_schedule))
        toolbox.register("update", componoud_update)

        pop, logbook, best = run_pso(toolbox=toolbox, **params)

        resulted_schedule = pf_schedule(best)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg


def create_pfgsa(wf, rm, estimator,
                init_sched_percent=0.05,
                **params):

    raise NotImplementedError()

    def alg(fixed_schedule_part, initial_schedule, current_time=0.0):
        def generate_(n):
            init_ind_count = int(n*init_sched_percent)
            res = []
            if initial_schedule is not None and init_ind_count > 0:
                heft_particle = gsa_generate(wf, rm, estimator, initial_schedule)
                init_arr = [deepcopy(heft_particle) for _ in range(init_ind_count)]
                res = res + init_arr
            if n - init_ind_count > 0:
                generated_arr = [gsa_generate(wf, rm, estimator,
                                          schedule=None,
                                          fixed_schedule_part=fixed_schedule_part,
                                          current_time=current_time)
                                 for _ in range(n - init_ind_count)]
                res = res + generated_arr
            return res

        def fit_converter(func):
            def wrap(*args, **kwargs):
                x = func(*args, **kwargs)
                m = Utility.makespan(x)
                return FitnessStd(values=(m, 0.0))
            return wrap

        def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
            mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
            ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)

        task_map = {task.id: task for task in wf.get_all_unique_tasks()}
        node_map = {node.name: node for node in rm.get_nodes()}

        schedule_builder = ParticleScheduleBuilder(wf, rm, estimator,
                                                   task_map, node_map,
                                                   fixed_schedule_part)
        pf_schedule = partial(schedule_builder, current_time=current_time)

        toolbox = Toolbox()
        toolbox.register("population", generate_)
        toolbox.register("fitness", fit_converter(pf_schedule))
        toolbox.register("update", componoud_update)

        pop, logbook, best = run_pso(toolbox=toolbox, **params)

        resulted_schedule = pf_schedule(best)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg


def create_ga_cleaner(wf, rm, estimator):
    return GaChromosomeCleaner(wf, rm, estimator)


def create_pso_cleaner(wf, rm, estimator):
    # return AlternativePSOChromosomeCleaner(wf, rm, estimator)
    return PSOChromosomeCleaner(wf, rm, estimator)

