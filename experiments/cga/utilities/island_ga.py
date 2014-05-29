## interface of island component
from copy import deepcopy
import random


class Island:
    def gen(self):
        pass
    def result(self):
        pass
    def species(self):
        pass
    pass

def run_island_ga(islands, migration_scheme, MAX_GEN, MIGRATION_PERIOD):
    ## TODO: rewrite this
    """
    This algorithm is synchronious, i.e. It is assummed
    that change of generations in all islands performs simultaneously.

    kwargs must contain the following arguments
    :islands = [..., ..., ] - collection of island-algorithm
    every island-algorithm have to support adding of observer function
    to be able to perform interchange between islands
    Observer have to be provided with current population
    (bundle of populations if there are several species)
    and archive if it exists.
    Every island contains emigrants chooser and immigrants chooser
    The first one decides which individuals of population can be
    moved to another islands.
    The second one is responsible for filtering of immigrants going to enter the island.

    :exchange_scheme - alg deciding when every island should send its emigrants.
    """

    for i in range(MAX_GEN):

        for island in islands:
            island.gen()

        if i > 0 and i % MIGRATION_PERIOD == 0:
            species = islands[0].species()
            separate_pops = [[island.pops[s] for island in islands] for s in species]
            for p in separate_pops:
                migration_scheme(p)

        pass

    bests = []
    for island in islands:
        best, pops, logbook, initial_pops = island.result()
        bests.append(best)
        pass

    best = max(bests, key=lambda x: x.fitness)
    return best, islands

def equal_social_migration_scheme(populations, k, selection):
    """
    In this scheme we get from every population :k elements and distribute their between all another islands.
    :k should be multiple by (count of islands - 1) to properly distribute individuals between islands
    """
    assert k % (len(populations) - 1) == 0, "k is not multiple by count of islands"
    emigrants = {i: selection(populations[i], k) for i in range(len(populations))}
    copy_emigrants = deepcopy(emigrants)

    def choose(emigr):
        el = emigr[random.randint(0, len(emigr) - 1)]
        emigr.remove(el)
        return el
    immigrants = [[choose(v) for k, v in emigrants.items() if k != i] for i in range(len(populations))]

    print([len(v) for k, v in copy_emigrants.items()])
    print([len(el) for el in immigrants])


    # pop_without_emigr = lambda pop, em: [ind for ind in pop if ind not in em]
    def pop_without_emigr(pop, em):
        print("VVV:")
        print([id(e) for e in em])

        np = deepcopy(pop)
        for e in em:
            np.remove(e)
        return np
        # return [ind for ind in pop if ind not in em]
        # return pop
    new_populations = [pop_without_emigr(pop, em) + im for pop, em, im in zip(populations, [x[1] for x in sorted(copy_emigrants.items(), key=lambda x: x[0])], immigrants)]

    ## fill old population with changed content
    for old_pop, new_pop in zip(populations, new_populations):
        assert len(old_pop) == len(new_pop), \
            "population with immigrants differs from old population by size: {0} != {1}".format(len(old_pop), len(new_pop))
        for i in range(len(old_pop)):
            old_pop[i] = new_pop[i]
    pass

def best_selection(pop, k):
    return sorted(pop, key=lambda x: x.fitness, reverse=True)[0:k]