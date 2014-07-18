import deap
from deap import tools, algorithms
from deap.base import Toolbox
import numpy
from algs.common.individuals import ListBasedIndividual
from algs.ga.coevolution.cga import Env
from algs.ga.coevolution.operators import mapping_heft_based_initialize, ordering_heft_based_initialize, ordering_default_crossover, mapping_default_mutate, ordering_default_mutate, fitness_mapping_and_ordering, MAPPING_SPECIE, ORDERING_SPECIE
from experiments.cga.utilities.common import Fitness


def _mate(ctx, child1, child2):
    ch1_mp, ch1_os = child1
    ch2_mp, ch2_os = child2
    tools.cxOnePoint(ch1_mp, ch2_mp)
    ordering_default_crossover(ctx, ch1_os, ch2_os)
    child1 = ListBasedIndividual((ch1_mp, ch1_os))
    child1.fitness = Fitness(0)
    child1.fitness.valid = False
    child2 = ListBasedIndividual((ch2_mp, ch2_os))
    child2.fitness = Fitness(0)
    child2.fitness.valid = False
    return child1, child2

def _mutate(ctx, mutant):
    mp, os = mutant
    mapping_default_mutate(ctx, mp)
    ordering_default_mutate(ctx, os)
    child = ListBasedIndividual((mp, os))
    child.fitness = Fitness(0)
    child.fitness.valid = False
    return (child,)



def run_dcga(wf, estimator, rm, heft_mapping, heft_ordering, **params):

    cxpb = params["cxpb"]#0.9
    mutpb = params["mutpb"]#0.9
    ngen = params["ngen"]#100
    pop_size = params["pop_size"]#100

    ctx = {'env': Env(wf, rm, estimator)}

    toolbox = Toolbox()
    toolbox.register("select", tools.selTournament, tournsize=2)
    toolbox.register("mate", _mate, ctx)
    toolbox.register("mutate", _mutate, ctx)
    toolbox.register("evaluate", lambda x: [fitness_mapping_and_ordering(ctx, {MAPPING_SPECIE: x[0], ORDERING_SPECIE: x[1]})])

    # heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json", rm)

    pop_mapping = mapping_heft_based_initialize(ctx, pop_size, heft_mapping, 3)
    pop_ordering = ordering_heft_based_initialize(ctx, pop_size, heft_ordering, 3)
    pop = [ListBasedIndividual(el) for el in zip(pop_mapping, pop_ordering)]
    for p in pop:
        p.fitness = Fitness(0)

    stat = tools.Statistics(key=lambda x: x.fitness)
    stat.register("solsstat", lambda pop: [{"best": numpy.max(pop).values[0]}])

    final_pop, logbook = deap.algorithms.eaSimple(pop, toolbox, cxpb, mutpb, ngen, stat)
    best = max(final_pop, key=lambda x: x.fitness)
    return best.fitness.values[0], logbook

