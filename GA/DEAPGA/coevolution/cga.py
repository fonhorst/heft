
"""
"""
import functools
import random


class Specie:
    def __init__(self, name, pop_size):
        self.name = name
        self.pop_size = pop_size
    pass

def create_cooperative_ga(toolbox, **kwargs):

    # create initial populations of all species
    # taking part in coevolution

    SPECIES = kwargs["species"]
    INTERACT_INDIVIDUALS_COUNT = kwargs["interact_individuals_count"]
    GENERATIONS = kwargs["generations"]

    def generate_k(pop):
        base_k = int(INTERACT_INDIVIDUALS_COUNT / len(pop))
        free_slots = INTERACT_INDIVIDUALS_COUNT % len(pop)
        for ind in pop:
            ind.k = base_k

        for slot in free_slots:
            i = random.randint(0, len(pop) - 1)
            pop[i].k += 1
        return pop

    def decrease_k(ind):
        ind.k -= 1
        return ind

    ## TODO: replace credits with pop
    ## TODO: add credit in inds of pop
    def credit_to_k(credits):
        norma = INTERACT_INDIVIDUALS_COUNT / sum(credits)
        partial_k = [int(c * norma) for c in credits]
        values = zip(credits, partial_k)
        left_part = INTERACT_INDIVIDUALS_COUNT - sum(partial_k)
        values = sorted(values, key=lambda x: x[0], reverse=True)
        for i in range(left_part):
            values[i] = (values[i][0], values[i][1] + 1)
        return values


    pops = {s: generate_k(toolbox.initialize(s, s.pop_size)) for s in SPECIES}

    best = None

    for gen in GENERATIONS:
        solutions = []
        for i in range(INTERACT_INDIVIDUALS_COUNT):
            solution = {s: decrease_k(toolbox.select(pop)) for s, pop in pops}
            solutions.append(solution)

        for sol in solutions:
            fit = toolbox.fitness(solution)
            sol.fitness = fit

        inds_credit = dict()
        for sol in solutions:
            for s, ind in sol.items():
                values = inds_credit.get(ind, [0, 0])
                values[0] += sol.fitness / len(sol)
                values[1] += 1
                inds_credit[ind] = values

        ## TODO: remake in more generic way
        ## TODO: add archive and corresponding updating and mixing
        best = min(solution, key=lambda x: x.fitness)

        inds_credit = {ind: values[0] / values[1] for ind, values in inds_credit.items()}


        sort = lambda seed, x: seed[x.specie].append((x[0], x[1]))
        species_with_credits = functools.reduce(sort,
                                                inds_credit.items(),
                                                {s: [] for s in SPECIES})


        for s, inds in species_with_credits.items():
            # TODO: implement it and replace pops with new pops
            # mating specific for the specie
            # mutation specific for the specie
            raise NotImplementedError()

        for s, pop in pops:
            distribute_k_depend_on_credit(pop, INTERACT_INDIVIDUALS_COUNT)
        pass
    return best













    pass
