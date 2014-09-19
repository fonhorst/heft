from copy import deepcopy
import random

from deap.base import Toolbox
from deap.tools import Statistics, Logbook
import numpy

from heft.algs.gsa.SimpleGsaScheme import run_gsa
from heft.algs.gsa.operators import G, Kbest
from heft.algs.gsa.ordering_mapping_operators import force, mapping_update, ordering_update, CompoundParticle, generate
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.pso.ordering_operators import fitness, build_schedule
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import repeat


_wf = wf("Montage_50")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=20, transfer_time=100)
heft_schedule = run_heft(_wf, rm, estimator)


heft_particle = generate(_wf, rm, estimator, heft_schedule)

heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.00 else generate(_wf, rm, estimator) for _ in range(n)]

# def heft_gen(n):
#     heft_count = int(n*0.05)
#     pop = [deepcopy(heft_particle) for _ in range(heft_count)]
#     for _ in range(n - heft_count):
#         variant = gen()
#         hp = deepcopy(heft_particle)
#         variant.ordering = hp.ordering
#         pop.append(variant)
#     return pop

pop_size = 50
iter_number = 300
kbest = pop_size
ginit = 10
W, C = 0.2, 0.5


def compound_force(p, pop, kbest, G):
    mapping_force = force(p.mapping, (p.mapping for p in pop), kbest, G)
    ordering_force = force(p.ordering, (p.ordering for p in pop), kbest, G)
    return (mapping_force, ordering_force)

def compound_update(w, c, p, min=-1, max=1):
    mapping_update(w, c, p.mapping)
    ordering_update(w, c,  p.ordering, min, max)
    pass


toolbox = Toolbox()
# toolbox.register("generate", generate, _wf, rm, estimator)
toolbox.register("generate", heft_gen)
toolbox.register("fitness", fitness, _wf, rm, estimator)
toolbox.register("estimate_force", compound_force)
toolbox.register("update", compound_update, W, C)
toolbox.register("G", G)
toolbox.register("kbest", Kbest)

stats = Statistics()
stats.register("min", lambda pop: numpy.min([p.fitness.mofit for p in pop]))
stats.register("avr", lambda pop: numpy.average([p.fitness.mofit for p in pop]))
stats.register("max", lambda pop: numpy.max([p.fitness.mofit for p in pop]))
stats.register("std", lambda pop: numpy.std([p.fitness.mofit for p in pop]))

logbook = Logbook()
logbook.header = ["gen", "G", "kbest"] + stats.fields





def do_exp():
    pop, _logbook, best = run_gsa(toolbox, stats, logbook, pop_size, 0, iter_number, None, kbest, ginit, **{"w":W, "c":C})

    schedule = build_schedule(_wf, rm, estimator,  best)
    Utility.validate_static_schedule(_wf, schedule)
    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
    return makespan


if __name__ == "__main__":
    # result = repeat(do_exp, 5)
    result = do_exp()
    print(result)
    pass
