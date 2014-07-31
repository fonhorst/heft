import random
from deap import tools
from deap.base import Toolbox
import numpy
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.sdpso import run_pso, generate, update, schedule_to_position
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import fitness, build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)
heft_mapping = schedule_to_position(heft_schedule)

heft_gen = lambda: heft_mapping if random.random() > 0.95 else generate(_wf, rm, estimator)

W, C1, C2 = 0.9, 0.5, 0.5
GEN, N = 100, 50


toolbox = Toolbox()
toolbox.register("population", heft_gen)
toolbox.register("fitness", fitness,  wf=wf, rm=rm, estimator=estimator)
toolbox.register("update", update)

stats = tools.Statistics(lambda ind: ind.fitness.values)
stats.register("avg", numpy.mean)
stats.register("std", numpy.std)
stats.register("min", numpy.min)
stats.register("max", numpy.max)

logbook= tools.Logbook()
logbook.header = ["gen", "evals"] + stats.fields

def do_exp():
    pop, log, best = run_pso(
        w=W, c1=C1, c2=C2,
        gen=GEN, n=N,
        toolbox=toolbox,
        stats=stats,
        logbook=logbook
    )

    best = best.entity
    solution = {MAPPING_SPECIE: list(zip(sorted_tasks, best)), ORDERING_SPECIE: sorted_tasks}
    schedule = build_schedule(_wf, estimator, rm, solution)
    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    pass

if __name__ == "__main__":
    do_exp()
    pass
