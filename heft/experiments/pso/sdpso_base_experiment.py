from copy import deepcopy
import random
from deap import tools
from deap.base import Toolbox
import numpy
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.sdpso import run_pso, generate, update, schedule_to_position, fitness, construct_solution, Particle, \
    Velocity
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)
heft_mapping = schedule_to_position(heft_schedule)
heft_mapping.velocity = Velocity({})

heft_gen = lambda n: [deepcopy(heft_mapping) if random.random() > 1.0 else generate(_wf, rm, estimator, 1)[0] for _ in range(n)]

W, C1, C2 = 0.0, 0.5, 0.5
GEN, N = 10000, 30



toolbox = Toolbox()
toolbox.register("population", heft_gen)
toolbox.register("fitness", fitness,  _wf, rm, estimator, sorted_tasks)
toolbox.register("update", update)

stats = tools.Statistics(lambda ind: ind.fitness.values[0])
stats.register("avg", numpy.mean)
stats.register("std", numpy.std)
stats.register("min", numpy.min)
stats.register("max", numpy.max)

logbook = tools.Logbook()
logbook.header = ["gen", "evals"] + stats.fields

def do_exp():
    pop, log, best = run_pso(
        toolbox=toolbox,
        logbook=logbook,
        stats=stats,
        gen_curr=0, gen_step=GEN, invalidate_fitness=True, pop=None,
        w=W, c1=C1, c2=C2, n=N,
    )

    best_position = best.entity
    solution = construct_solution(best_position, sorted_tasks)
    schedule = build_schedule(_wf, estimator, rm, solution)
    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
    pass

if __name__ == "__main__":
    do_exp()
    pass
