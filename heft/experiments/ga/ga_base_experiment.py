from copy import deepcopy
from functools import partial
from deap import tools
from deap.base import Toolbox
import numpy
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.ga.common_fixed_schedule_schema import run_ga, fit_converter
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.algs.ga.common_fixed_schedule_schema import generate as ga_generate

wf_name = "Montage_25"

GA_PARAMS = {
        "kbest": 5,
        "n": 50,
        "cxpb": 0.3,  # 0.8
        "mutpb": 0.1,  # 0.5
        "sweepmutpb": 0.3,  # 0.4
        "gen_curr": 0,
        "gen_step": 300,
        "is_silent": False
}

def do_exp(wf_name):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
    estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                                ideal_flops=20, transfer_time=100)

    empty_fixed_schedule_part = Schedule({node: [] for node in rm.get_nodes()})

    heft_schedule = run_heft(_wf, rm, estimator)

    ga_functions = GAFunctions2(_wf, rm, estimator)

    generate = partial(ga_generate, ga_functions=ga_functions,
                               fixed_schedule_part=empty_fixed_schedule_part,
                               current_time=0.0, init_sched_percent=0.05,
                               initial_schedule=heft_schedule)


    stats = tools.Statistics(lambda ind: ind.fitness.values[0])
    stats.register("avg", numpy.mean)
    stats.register("std", numpy.std)
    stats.register("min", numpy.min)
    stats.register("max", numpy.max)

    logbook = tools.Logbook()
    logbook.header = ["gen", "evals"] + stats.fields

    toolbox = Toolbox()
    toolbox.register("generate", generate)
    toolbox.register("evaluate", fit_converter(ga_functions.build_fitness(empty_fixed_schedule_part, 0.0)))
    toolbox.register("clone", deepcopy)
    toolbox.register("mate", ga_functions.crossover)
    toolbox.register("sweep_mutation", ga_functions.sweep_mutation)
    toolbox.register("mutate", ga_functions.mutation)
    # toolbox.register("select_parents", )
    # toolbox.register("select", tools.selTournament, tournsize=4)
    toolbox.register("select", tools.selRoulette)
    pop, logbook, best = run_ga(toolbox=toolbox,
                                logbook=logbook,
                                stats=stats,
                                **GA_PARAMS)

    resulted_schedule = ga_functions.build_schedule(best, empty_fixed_schedule_part, 0.0)

    Utility.validate_static_schedule(_wf, resulted_schedule)

    ga_makespan = Utility.makespan(resulted_schedule)
    return ga_makespan

if __name__ == "__main__":
    result = do_exp(wf_name)
    print(result)