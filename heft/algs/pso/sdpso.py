"""
This is a prototype of set-based PSO algorithm directed to solve workflow scheduling problem with fixed ordering.
It must be refactored and made in reusable, configurable form later.

Position =>{task_name: node_name}
Velocity => {(task_name, node_name): probability}
"""
from copy import deepcopy
import random
import math

from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.common.mapordschedule import fitness as basefitness
from heft.algs.common.utilities import gather_info
from heft.core.environment.Utility import timing, RepeatableTiming


#@RepeatableTiming(100)
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

    #print("PSO_STARTED")

    # pso = MappingPSO(w, c1, c2, gen, n, toolbox, stats, logbook)
    # return pso()

    pop = initial_pop

    w, c1, c2 = params["w"], params["c1"], params["c2"]

    n = len(pop) if pop is not None else params["n"]
    res_list = [0 for _ in range(gen_step)]
    ## TODO: remove it later
    #w = w * (500 - gen_curr)/500

    best = params.get('best', None)

    #curFile = open("C:\Melnik\Experiments\Work\PSO_compare" + "\\"  +  "SD generations.txt", 'w')

    hallOfFame = []
    hallOfFameSize = int(math.log(n))
    if hallOfFameSize == 0:
        print("need more particles")

    bestIndex = 0
    changeChance = 0.1

    if pop is None:
        pop = toolbox.population(n)

    for g in range(gen_curr, gen_curr + gen_step, 1):
        #print("g: {0}".format(g))
        for p in pop:
            if not hasattr(p, "fitness") or not p.fitness.valid:
                p.fitness = toolbox.fitness(p)
            if not p.best or p.best.fitness < p.fitness:
                p.best = deepcopy(p)

            if not best or hallOfFame[hallOfFameSize-1].fitness < p.fitness:
                hallOfFame = changeHall(hallOfFame, p, hallOfFameSize)
            #if not best or best.fitness < p.fitness:
             #   best = deepcopy(p)
        # Gather all the fitnesses in one list and print the stats
        #gather_info(logbook, stats, g, pop)

        #curFile.write(str(g) + "   " + str(best.fitness.values[0]) + "\n")

        bestIndex = changeIndex(bestIndex, changeChance, hallOfFameSize)
        best = hallOfFame[bestIndex]

        min_value = min([part.fitness.values[0] for part in hallOfFame])
        res_list[g] = res_list[g] + min_value
        print("g:" + str(g) + " " + str(min_value))


        """
        hallString = [part.fitness.values[0] for part in hallOfFame]
        curFile.write(str(g) + "   " + str(bestIndex) + "    " + str(hallString) +  "\n")
        particlesFitness = sorted([part.fitness.values[0] for part in pop])
        curFile.write(str(particlesFitness) + "\n")
        curFile.write("\n")
        """


        for p in pop:
            toolbox.update(w, c1, c2, p, best, pop)
        if invalidate_fitness and not g == gen_step-1:
            for p in pop:
                del p.fitness
        pass

    """
    hallString = [part.fitness.values[0] for part in hallOfFame]
    curFile.write(str("FINAL") + "   " + str(bestIndex) + "    " + str(hallString) +  "\n")
    particlesFitness = sorted([part.fitness.values[0] for part in pop])
    curFile.write(str(particlesFitness) + "\n")
    curFile.write("\n")
    curFile.close()
    """

    #curFile.close()



    hallOfFame.sort(key=lambda p:p.fitness, reverse=True)
    best = hallOfFame[0]

    return pop, logbook, best, res_list


def velocity_update(w, c1, c2, pbest, gbest, velocity, particle, pop):
    r1 = random.random()
    r2 = random.random()

    old_velocity = velocity*w
    pbest_velocity = (pbest - particle)*(c2*r2)
    gbest_velocity = (gbest - particle)*(c1*r1)

    new_velocity = old_velocity + pbest_velocity + gbest_velocity
    return new_velocity

def changeHall(hall, part, size):
    if part.fitness in [p.fitness for p in hall]:
        return hall
    hall.append(deepcopy(part))
    hall.sort(key=lambda p: p.fitness, reverse=True)
    return hall[0:size]

def changeIndex(idx, chance, size):
    if random.random() < chance:
        rnd = int(random.random() * size)
        while (rnd == idx):
            rnd = int(random.random() * size)
        return rnd
    return idx






