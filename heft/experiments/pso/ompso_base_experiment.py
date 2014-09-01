from copy import deepcopy
from functools import partial
import random
from deap import tools
from deap.base import Toolbox
import numpy
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.ompso import run_ompso, fitness, generate, construct_solution, ordering_update
from heft.algs.pso.sdpso import run_pso, update, schedule_to_position
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE, ord_and_map
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.cga.utilities.common import repeat

_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)

sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)

heft_particle = generate(_wf, rm, estimator, heft_schedule)

heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.0 else generate(_wf, rm, estimator) for _ in range(n)]

W, C1, C2 = 0.9, 0.6, 0.2
GEN, N = 500, 30


def check_fitness(x):
    if hasattr(x, "fitness") and x.fitness is not None and x.fitness.valid:
        return x.fitness
    raise ValueError("Fitness is not valid")


def build_pso_mapping_alg():
    toolbox = Toolbox()

    def population(n):
        raise Exception("This function mustn't have been called")

    def switching_update(w, c1, c2, p, best, pop):
        return update(w, c1, c2, p.mapping, best.mapping, pop)

    toolbox.register("population", population)
    toolbox.register("fitness", check_fitness)
    toolbox.register("update", switching_update)

    alg = partial(run_pso, toolbox=toolbox,
                  w=W, c1=C1, c2=C2, n=N)
    return alg

def build_pso_ordering_alg():
    toolbox = Toolbox()

    def population(n):
        raise Exception("This function mustn't have been called")

    # def switching_update(w, c1, c2, p, best, pop):
    #     return update(w, c1, c2, p.mapping, best.mapping, pop)

    toolbox.register("population", population)
    toolbox.register("fitness", check_fitness)
    toolbox.register("update", ordering_update)

    alg = partial(run_pso, toolbox=toolbox,
                  w=W, c1=C1, c2=C2, n=N)
    return alg

def build_VNS_alg():
    return None
    # raise NotImplementedError()



toolbox = Toolbox()

toolbox.register("generate", heft_gen)
toolbox.register("fitness", fitness, _wf, rm, estimator)
toolbox.register("pso_mapping", build_pso_mapping_alg())
toolbox.register("pso_ordering", build_pso_ordering_alg())
toolbox.register("VNS", build_VNS_alg())

stats = tools.Statistics(lambda ind: ind.fitness.values[0])
stats.register("avg", numpy.mean)
stats.register("std", numpy.std)
stats.register("min", numpy.min)
stats.register("max", numpy.max)

logbook = tools.Logbook()
logbook.header = ["gen", "evals"] + stats.fields





def do_exp():

    pop, log, best = run_ompso(
        toolbox=toolbox,
        logbook=logbook,
        stats=stats,
        gen_curr=0, gen_step=GEN, invalidate_fitness=True, pop=None,
        n=N,
    )



    mapping, ordering = best.mapping.entity, best.ordering.entity
    solution = construct_solution(mapping, ordering)
    schedule = build_schedule(_wf, estimator, rm, solution)

    Utility.validate_static_schedule(_wf, schedule)

    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
    return makespan

if __name__ == "__main__":
    # result = repeat(do_exp, 1)
    result = do_exp()
    print(result)
    pass

