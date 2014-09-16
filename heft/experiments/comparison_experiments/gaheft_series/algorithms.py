from copy import deepcopy
from functools import partial

from deap import tools
from deap.base import Toolbox
from deap.tools.migration import migRing

from heft.algs.common.individuals import FitnessStd
from heft.algs.common.particle_operations import CompoundParticle
from heft.algs.ga.GAImplementation.GAImpl import GAFactory
from heft.algs.ga.common_fixed_schedule_schema import run_ga, fit_converter
from heft.algs.ga.common_fixed_schedule_schema import generate as ga_generate
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.ga.multipopGA.MPGA import create_mpga
from heft.algs.ga.multipopGA.mpga_refactored import run_mpga
from heft.algs.gsa.SimpleGsaScheme import run_gsa
from heft.algs.gsa.operators import G, Kbest
from heft.algs.gsa.ordering_mapping_operators import force
from heft.algs.pso.mapping_operators import update as mapping_update
from heft.algs.pso.ordering_operators import generate as pso_generate, ordering_update
from heft.algs.pso.sdpso import run_pso
from heft.core.environment.Utility import Utility
from heft.experiments.comparison_experiments.common.chromosome_cleaner import GaChromosomeCleaner, PSOChromosomeCleaner
from heft.experiments.comparison_experiments.gaheft_series.utilities import ParticleScheduleBuilder, emigrant_selection
from heft.experiments.comparison_experiments.gaheft_series.utilities import generate
from heft.algs.gsa.ordering_mapping_operators import CompoundParticle as GsaCompoundParticle
from heft.algs.gsa.ordering_mapping_operators import mapping_update as gsa_mapping_update
from heft.algs.gsa.ordering_mapping_operators import ordering_update as gsa_ordering_update
from heft.algs.gsa.ordering_mapping_operators import generate as gsa_generate


def create_old_ga(wf, rm, estimator,
                  init_sched_percent=0.05,
                  log_book=None, stats=None,
                  alg_params=None):
    kwargs = {}
    kwargs["wf"] = wf
    kwargs["resource_manager"] = rm
    kwargs["estimator"] = estimator
    kwargs["ga_params"] = {
        "population": alg_params["n"],
        "crossover_probability": alg_params["cxpb"],
        "replacing_mutation_probability": alg_params["mutpb"],
        "generations": alg_params["gen_step"],
        "sweep_mutation_probability": alg_params["sweepmutpb"],
        "Kbest": alg_params["kbest"]
    }
    kwargs["silent"] = alg_params["is_silent"]
    ga = partial(GAFactory.default().create_ga, **kwargs)
    return ga()


def create_pfga(wf, rm, estimator,
                init_sched_percent=0.05,
                log_book=None, stats=None,
                alg_params=None):
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
        pop, logbook, best = run_ga(toolbox=toolbox, **alg_params)

        resulted_schedule = ga_functions.build_schedule(best, fixed_schedule_part, current_time)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg

def create_pso_alg(pf_schedule, generate_, **params):
    def fit_converter(func):
        def wrap(*args, **kwargs):
            x = func(*args, **kwargs)
            m = Utility.makespan(x)
            return FitnessStd(values=(m, 0.0))
        return wrap

    def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
        mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
        ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)


    toolbox = Toolbox()
    toolbox.register("population", generate_)
    toolbox.register("fitness", fit_converter(pf_schedule))
    toolbox.register("update", componoud_update)

    pso_alg = partial(run_pso, toolbox=toolbox, **params)
    return pso_alg


## TODO: remake interface later
## do NOT use it for anything except 'gaheft_series' experiments
def create_pfpso(wf, rm, estimator,
                 init_sched_percent=0.05,
                 log_book=None, stats=None,
                 alg_params=None):
    def alg(fixed_schedule_part, initial_schedule, current_time=0.0, initial_population=None):


        generate_ = partial(generate, wf=wf, rm=rm, estimator=estimator,
                            fixed_schedule_part=fixed_schedule_part, current_time=current_time,
                            base_generate=pso_generate,
                            init_sched_percent=init_sched_percent,
                            initial_schedule=initial_schedule, initial_population=initial_population)

        task_map = {task.id: task for task in wf.get_all_unique_tasks()}
        node_map = {node.name: node for node in rm.get_nodes()}
        schedule_builder = ParticleScheduleBuilder(wf, rm, estimator,
                                                   task_map, node_map, fixed_schedule_part)

        pf_schedule = partial(schedule_builder, current_time=current_time)

        lb, st = deepcopy(log_book), deepcopy(stats)

        pso = create_pso_alg(pf_schedule, generate_, logbook=lb, stats=st, **alg_params)
        pop, logbook, best = pso()

        resulted_schedule = pf_schedule(best)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg

def create_gsa_alg(pf_schedule, generate_, **params):
    def fit_converter(func):
        def wrap(*args, **kwargs):
            x = func(*args, **kwargs)
            m = Utility.makespan(x)
            return FitnessStd(values=(m, 0.0))
        return wrap

    def compound_force(p, pop, kbest, G):
        mapping_force = force(p.mapping, (p.mapping for p in pop), kbest, G)
        ordering_force = force(p.ordering, (p.ordering for p in pop), kbest, G)
        return (mapping_force, ordering_force)

    def compound_update(w, c, p, min=-1, max=1):
        gsa_mapping_update(w, c, p.mapping)
        gsa_ordering_update(w, c,  p.ordering, min, max)
        pass

    W, C = params["w"], params["w"]

    all_iterations_count = int(params["generations_count_before_merge"]) + int(params["generations_count_after_merge"])

    toolbox = Toolbox()
    toolbox.register("generate", generate_)
    toolbox.register("fitness", fit_converter(pf_schedule))
    toolbox.register("estimate_force", compound_force)
    toolbox.register("update", compound_update, W, C)
    toolbox.register("G", partial(G, all_iter_number=all_iterations_count))
    toolbox.register("kbest", partial(Kbest, all_iter_number=all_iterations_count))

    pso_alg = partial(run_gsa, toolbox=toolbox, **params)
    return pso_alg


def create_pfgsa(wf, rm, estimator,
                 init_sched_percent=0.05,
                 log_book=None, stats=None,
                 alg_params=None):
    def alg(fixed_schedule_part, initial_schedule, current_time=0.0, initial_population=None):

        def gsa_generate(*args, **kwargs):
            particle = pso_generate(*args, **kwargs)
            particle = GsaCompoundParticle(particle.mapping, particle.ordering)
            return particle

        generate_ = partial(generate, wf=wf, rm=rm, estimator=estimator,
                            fixed_schedule_part=fixed_schedule_part, current_time=current_time,
                            base_generate=gsa_generate,
                            init_sched_percent=init_sched_percent,
                            initial_schedule=None, initial_population=initial_population)


        task_map = {task.id: task for task in wf.get_all_unique_tasks()}
        node_map = {node.name: node for node in rm.get_nodes()}
        schedule_builder = ParticleScheduleBuilder(wf, rm, estimator,
                                                   task_map, node_map, fixed_schedule_part)

        pf_schedule = partial(schedule_builder, current_time=current_time)

        lb, st = deepcopy(log_book), deepcopy(stats)

        pso = create_gsa_alg(pf_schedule, generate_, logbook=lb, stats=st, kbest=alg_params["n"], **alg_params)
        pop, logbook, best = pso()

        resulted_schedule = pf_schedule(best)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg


def create_old_pfmpga(wf, rm, estimator,
                init_sched_percent=0.05,
                log_book=None, stats=None,
                algorithm=None,
                alg_params=None):

    "merged_pop_iters"
    "migrCount"
    "all_iters_count"




    kwargs = {}
    kwargs["wf"] = wf
    kwargs["resource_manager"] = rm
    kwargs["estimator"] = estimator
    kwargs["ga_params"] = {
        "population": alg_params["n"],
        "crossover_probability": alg_params["cxpb"],
        "replacing_mutation_probability": alg_params["mutpb"],
        "generations": alg_params["gen_step"],
        "sweep_mutation_probability": alg_params["sweepmutpb"],
        "Kbest": alg_params["kbest"],
        "merged_pop_iters": alg_params["merged_pop_iters"]
    }
    kwargs["silent"] = alg_params["is_silent"]
    kwargs["migrCount"] = alg_params["migrCount"]
    kwargs["emigrant_selection"] = emigrant_selection
    kwargs["all_iters_count"] = alg_params["all_iters_count"]
    ga = partial(create_mpga, **kwargs)
    return ga()


def create_pfmpga(wf, rm, estimator,
                  init_sched_percent=0.05,
                  log_book=None, stats=None,
                  algorithm=None,
                  generate_func=None,
                  alg_params=None):
    if algorithm is None:
        raise ValueError("Algorithm cannot be none")
    def alg(fixed_schedule_part, initial_schedule, current_time=0.0, initial_population=None, only_new_pops=False):

        n = alg_params["n"]

        ### generate heft_based population
        init_ind_count = int(n * init_sched_percent)
        heft_particle = initial_schedule if isinstance(initial_schedule, (CompoundParticle, GsaCompoundParticle)) \
            else generate_func(wf, rm, estimator, initial_schedule)
        init_arr = [deepcopy(heft_particle) for _ in range(init_ind_count)]
        generated_arr = [generate_func(wf, rm, estimator,
                                          schedule=None,
                                          fixed_schedule_part=fixed_schedule_part,
                                          current_time=current_time)
                                 for _ in range(n - init_ind_count)]
        heft_based_population = init_arr + generated_arr

        ### generate new population
        random_population = [generate_func(wf, rm, estimator,
                                          schedule=None,
                                          fixed_schedule_part=fixed_schedule_part,
                                          current_time=current_time)
                                 for _ in range(n)]

        populations = {
            #"inherited": initial_population,
            #"heft_based": heft_based_population,
            "random": random_population
        }

        if "inherited" in populations and(populations["inherited"] is None or len(populations["inherited"])):
            del populations["inherited"]

        def migration(populations, k):
            pops = [pop for name, pop in sorted(populations.items(), key=lambda x: x[0])]
            migRing(pops, k, selection=emigrant_selection)



        task_map = {task.id: task for task in wf.get_all_unique_tasks()}
        node_map = {node.name: node for node in rm.get_nodes()}
        schedule_builder = ParticleScheduleBuilder(wf, rm, estimator,
                                                   task_map, node_map,
                                                   fixed_schedule_part)
        pf_schedule = partial(schedule_builder, current_time=current_time)

        toolbox = Toolbox()
        toolbox.register("run_alg", algorithm(pf_schedule=pf_schedule, generate_=lambda n: None, **alg_params))
        toolbox.register("migration", migration)

        lb, st = deepcopy(log_book), deepcopy(stats)

        pop, logbook, best = run_mpga(toolbox=toolbox, logbook=lb, stats=st, initial_populations=populations, **alg_params)
        resulted_schedule = pf_schedule(best)
        result = (best, pop, resulted_schedule, None), logbook
        return result
    return alg


def create_ga_cleaner(wf, rm, estimator):
    return GaChromosomeCleaner(wf, rm, estimator)


def create_pso_cleaner(wf, rm, estimator):
    # return AlternativePSOChromosomeCleaner(wf, rm, estimator)
    return PSOChromosomeCleaner(wf, rm, estimator)


def create_schedule_to_ga_chromosome_converter(wf, rm, estimator):
    return GAFunctions2.schedule_to_chromosome


def create_schedule_to_pso_chromosome_converter(wf, rm, estimator):
    return partial(pso_generate, wf, rm, estimator)

def create_schedule_to_gsa_chromosome_converter(wf, rm, estimator):
    return partial(gsa_generate, wf, rm, estimator)



