"""
This is a prototype of set-based PSO algorithm directed to solve workflow scheduling problem with fixed ordering.
It must be refactored and made in reusable, configurable form later.

Position =>{task_name: node_name}
Velocity => {(task_name, node_name): probability}
"""
from copy import deepcopy
import random

from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.common.mapordschedule import fitness as basefitness
from heft.algs.common.utilities import gather_info


def run_pso(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, initial_pop=None, **params):

    """
    :param w:
    :param c1:
    :param c2:
    :param gen:
    :param n:
    :param toolbox:
    :param stats:
    :param logbook:
    :return:

    for toolbox we need the following functions:
    population
    fitness
    update

    And the following params:
    w
    c1
    c2
    n
    """

    print("PSO_STARTED")

    # pso = MappingPSO(w, c1, c2, gen, n, toolbox, stats, logbook)
    # return pso()

    pop = initial_pop

    w, c1, c2 = params["w"], params["c1"], params["c2"]
    n = len(pop) if pop is not None else params["n"]
    ## TODO: remove it later
    #w = w * (500 - gen_curr)/500

    best = params.get('best', None)

    if pop is None:
        pop = toolbox.population(n)

    for g in range(gen_curr, gen_curr + gen_step, 1):
        print("g: {0}".format(g))
        for p in pop:
            if not hasattr(p, "fitness") or not p.fitness.valid:
                p.fitness = toolbox.fitness(p)
            if not p.best or p.best.fitness < p.fitness:
                p.best = deepcopy(p)
            if not best or best.fitness < p.fitness:
                best = deepcopy(p)
        # Gather all the fitnesses in one list and print the stats
        gather_info(logbook, stats, g, pop)
        for p in pop:
            toolbox.update(w, c1, c2, p, best, pop)
        if invalidate_fitness:
            for p in pop:
                del p.fitness
        pass
    return pop, logbook, best


def velocity_update(w, c1, c2, pbest, gbest, velocity, particle, pop):
    r1 = random.random()
    r2 = random.random()

    old_velocity = velocity*w
    pbest_velocity = (pbest - particle)*(c1*r1)
    gbest_velocity = (gbest - particle)*(c2*r2)

    new_velocity = old_velocity + pbest_velocity + gbest_velocity
    return new_velocity








