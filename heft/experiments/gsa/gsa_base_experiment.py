from copy import deepcopy
import random
from deap import creator
from deap.base import Toolbox
from deap.tools import Statistics, Logbook
import numpy
from heft.algs.common.individuals import FitnessStd
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE, build_schedule

from heft.algs.gsa.SimpleGsaScheme import run_gsa
from heft.algs.gsa.operators import G, Kbest
from heft.algs.gsa.setbasedoperators import force_vector_matrix, velocity_and_position
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.sdpso import schedule_to_position, generate, fitness
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import repeat

_wf = wf("Montage_50")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)
heft_mapping = schedule_to_position(heft_schedule)

heft_gen = lambda n: [deepcopy(heft_mapping) if random.random() > 1.0 else generate(_wf, rm, estimator, 1)[0] for _ in range(n)]

toolbox = Toolbox()
# toolbox.register("generate", generate, _wf, rm, estimator)
toolbox.register("generate", heft_gen)
toolbox.register("fitness", fitness, _wf, rm, estimator, sorted_tasks)
toolbox.register("force_vector_matrix", force_vector_matrix)
toolbox.register("velocity_and_position", velocity_and_position)
toolbox.register("G", G)
toolbox.register("kbest", Kbest)

stats = Statistics()
stats.register("min", lambda pop: numpy.min([p.fitness.mofit for p in pop]))
stats.register("avr", lambda pop: numpy.average([p.fitness.mofit for p in pop]))
stats.register("max", lambda pop: numpy.max([p.fitness.mofit for p in pop]))
stats.register("std", lambda pop: numpy.std([p.fitness.mofit for p in pop]))

logbook = Logbook()
logbook.header = ("gen", "G", "kbest", "min", "avr", "max", "std")



pop_size = 30
iter_number = 200
kbest = pop_size
ginit = 2

def do_exp():
    pop, _logbook, best = run_gsa(toolbox, stats, logbook, pop_size, iter_number, kbest, ginit)
    solution = {MAPPING_SPECIE: list(best.entity.items()), ORDERING_SPECIE: sorted_tasks}
    schedule = build_schedule(_wf, estimator, rm, solution)
    Utility.validate_static_schedule(_wf, schedule)
    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    print("Final makespan: {0}".format(makespan))
    return makespan


if __name__ == "__main__":
    result = repeat(do_exp, 1)
    print(result)
    pass
