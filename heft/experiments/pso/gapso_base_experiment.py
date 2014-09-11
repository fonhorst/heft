from copy import deepcopy
from functools import partial
import random
from deap import tools
from deap import creator
from deap.base import Toolbox
import numpy
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter, FitnessStd
from heft.algs.ga.coevolution.cga import Env
from heft.algs.ga.coevolution.operators import ordering_default_crossover, ordering_default_mutate, \
    ordering_heft_based_initialize
from heft.algs.ga.nsga2 import run_nsga2
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.gapso import run_gapso
from heft.algs.pso.sdpso import run_pso, update, schedule_to_position, construct_solution, MappingParticle, \
    Velocity, Position
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE, ordering_from_schedule, \
    mapping_from_schedule
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.algs.common.mapordschedule import fitness as basefitness

_wf = wf("Montage_50")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)

print(Utility.makespan(heft_schedule))




stats = tools.Statistics(lambda ind: ind.fitness.values[0])
stats.register("avg", numpy.mean)
stats.register("std", numpy.std)
stats.register("min", numpy.min)
stats.register("max", numpy.max)

logbook = tools.Logbook()
logbook.header = ["gen", "evals"] + stats.fields


heft_mapping = mapping_from_schedule(heft_schedule)
heft_ordering = ordering_from_schedule(heft_schedule)




# common params
GEN, N = 1000, 30
# pso params
W, C1, C2 = 0.0, 0.3, 0.3
# ga params
CXPB, MU = 0.1, N

class ParticleIndividual(FitAdapter):
    def __init__(self):
        velocity = Velocity({})
        best = None
        ordering = None
    pass


heft_particle = ParticleIndividual(Position(heft_mapping))
heft_particle.ordering = heft_ordering

def generate(n):
    schedules = [SimpleRandomizedHeuristic(_wf, rm.get_nodes(), estimator).schedule() for _ in range(n)]
    mapping_positions = [schedule_to_position(s).entity for s in schedules]
    ordering_individuals = [ordering_from_schedule(s) for s in schedules]
    pop = []
    for mp, os in zip(mapping_positions, ordering_individuals):
        p = ParticleIndividual(Position(mp))
        p.ordering = os
        pop.append(p)
    return pop


def population(n):
    return [deepcopy(heft_particle) if random.random() > 0.95 else generate(1)[0] for _ in range(n)]


def mutate(ind):
    os = ind.ordering
    ordering_default_mutate({'env': Env(wf=_wf, rm=rm, estimator=estimator)}, os)
    child = ParticleIndividual(ind.entity)
    child.ordering = os
    return child,


def mate(ind1, ind2):
    ch1_os = ind1.ordering
    ch2_os = ind2.ordering
    ordering_default_crossover(None, ch1_os, ch2_os)
    child1 = ParticleIndividual(ind1.entity)
    child1.ordering = ch1_os
    child2 = ParticleIndividual(ind2.entity)
    child2.ordering = ch2_os
    return child1, child2


def fitness(particleind):
    position = particleind.entity
    ordering = particleind.ordering
    solution = construct_solution(position, ordering)

    sched = build_schedule(_wf, estimator, rm, solution)
    makespan = Utility.makespan(sched)
    ## TODO: make a real estimation later
    fit = FitnessStd(values=(makespan, 0.0))
    ## TODO: make a normal multi-objective fitness estimation
    fit.mofit = makespan
    return fit

    return basefitness(_wf, rm, estimator, solution)


toolbox = Toolbox()
# common functions
toolbox.register("map", map)
toolbox.register("clone", deepcopy)
toolbox.register("population", population)
toolbox.register("fitness", fitness)

# pso functions
toolbox.register("update", update)
# ga functions
toolbox.register("mutate", mutate)
toolbox.register("mate", mate)
toolbox.register("select", tools.selNSGA2)

ga = partial(run_nsga2, toolbox=toolbox, logbook=None, stats=None,
             n=N, crossover_probability=CXPB, mutation_probability=MU)

pso = partial(run_pso, toolbox=toolbox, logbook=None, stats=None,
              invalidate_fitness=False,
              w=W, c1=C1, c2=C2, n=N)


def do_exp():
    pop, log, best = run_gapso(
        toolbox=toolbox,
        logbook=logbook,
        stats=stats,
        gen=GEN, n=N,  ga=ga, pso=pso
    )

    best_position = best.entity
    solution = construct_solution(best_position, sorted_tasks)
    schedule = build_schedule(_wf, estimator, rm, solution)
    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    pass

if __name__ == "__main__":
    do_exp()
    pass
