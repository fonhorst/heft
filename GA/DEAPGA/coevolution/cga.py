from collections import namedtuple
from copy import deepcopy
import random

from deap import creator, tools

SPECIES = "species"
OPERATORS = "operators"
DEFAULT = "default"
#
# def TBX(**kwargs):
#     toolbox = base.Toolbox()
#     species = kwargs[SPECIES]
#     for k, v in kwargs.items():
#         if k != OPERATORS:
#             toolbox.register(k, v)
#     for k, v in kwargs[OPERATORS].items():
#         if isinstance(v, dict):
#             base_value = v.get(DEFAULT, None)
#             for s in species:
#                 value = v.get(s, None)
#                 value = base_value if value is None else value
#                 assert value is not None, "Cannot find default or provided value for " + OPERATORS + "." + str(k) + "." + str(s)
#                 toolbox.register(k, value)
#         # elif isinstance(v, list):
#         #     assert len(v) > 0, "List param isn't right"
#         #     value = v[0]
#         #     # make partial application
#         else:
#             assert value is not None, "Cannot find default or provided value for " + OPERATORS + "." + str(k)
#             toolbox.register(k, value)
#     return toolbox

"""
The algorithm have to satisfy the next requirements:
 - arbitrary count of species
 - arbitrary species, so using of operators provided with toolbox interface
 - unit-testing
 - gathering statistics
 - logging

Optional:
 - tools for checking of pareto-front characteristics
 - tools for checking of diversity in population

 Optional 2:
 - different schemes of credit assignments
 - different schemes of interactions between populations
 - different schemes of credit inheritance(local fitness inheritance)



It is supposed that DSL of the algorithm looks like somehow below:
{
    Species(

    )
    SPECIES = toolbox.species,
    INTERACT_INDIVIDUALS_COUNT = toolbox.interact_individuals_count,
    GENERATIONS = toolbox.generations,
    CXB[s] = toolbox.crossover_probability,
    MUTATION[s] = toolbox.mutation_probability,
    toolbox.initialize(s, s.pop_size),
    toolbox.choose(pop),
    toolbox.fitness(solution),
    toolbox.select(s, pop),
    toolbox.clone
    toolbox.mate(s, child1, child2)
    toolbox.mutate(s, mutant)
}
toolbox = TBX(
{
    "species": [s1=Specie(),s2=Specie(),...]
    INTERACT_INDIVIDUALS_COUNT: ...,
    GENERATIONS: ...,
    "operators":{
        "fitness",
        "choose"
    }

})
"""



creator.create("DictBasedIndividual", dict)
DictBasedIndividual = creator.DictBasedIndividual

class Specie:
    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        ## TODO: reconsider, should It be here?
        self.pop_size = kwargs["pop_size"]
        ## crossover probability
        self.cxb = kwargs["cxb"]
        ## mutation probability
        self.mb = kwargs["mb"]

        self.fixed = kwargs.get("fixed", None)
        self.representative_individual = kwargs.get("representative_individual", None)
    pass

"""
Toolbox need to implement functions:

"""

Env = namedtuple('Env', ['wf', 'rm', 'estimator'])

class Specie:
    def __init__(self, **kwargs):
        if kwargs.get("fixed", False):
            self.fixed = True
            self.representative_individual = kwargs["representative_individual"]
        else:
            self.fixed = False
            self.name = kwargs["name"]
            self.pop_size = kwargs["pop_size"]
            self.mate = kwargs["mate"]
            self.mutate = kwargs["mutate"]
            self.select = kwargs["select"]
            self.initialize = kwargs["initialize"]
            self.cxb = kwargs["cxb"]
            self.mb = kwargs["mb"]
        pass


def logpops(logbook, gen, pops):
    for s, pop in pops.items():
        

    pass

def logsolutions(logbook, gen, solutions):
    pass



def create_cooperative_ga(**kwargs):
    """
    DSL example:
    workflow, resource_manager, estimator
    """
    def func():
        ## TODO: add ability to determine if evolution has stopped
        ## TODO: add archive
        ## TODO: add generating and collecting different statistics
        ## TODO: add logging
        # create initial populations of all species
        # taking part in coevolution

        ENV = kwargs["env"]
        SPECIES = kwargs["species"]
        INTERACT_INDIVIDUALS_COUNT = kwargs["interact_individuals_count"]
        GENERATIONS = kwargs["generations"]

        choose = kwargs["operators"]["choose"]
        fitness = kwargs["operators"]["fitness"]

        logbook = tools.Logbook()

        def generate_k(pop):
            base_k = int(INTERACT_INDIVIDUALS_COUNT / len(pop))
            free_slots = INTERACT_INDIVIDUALS_COUNT % len(pop)
            for ind in pop:
                ind.k = base_k

            for slot in range(free_slots):
                i = random.randint(0, len(pop) - 1)
                pop[i].k += 1
            return pop

        def decrease_k(ind):
            ind.k -= 1
            return ind

        def credit_to_k(pop):
            norma = INTERACT_INDIVIDUALS_COUNT / sum(el.fitness for el in pop)
            for c in pop:
                c.k = int(c.fitness * norma)
            left_part = INTERACT_INDIVIDUALS_COUNT - sum(c.k for c in pop)
            sorted_pop = sorted(pop, key=lambda x: x.fitness, reverse=True)
            for i in range(left_part):
                sorted_pop[i].k += 1
            return pop

        pops = {s: generate_k(s.initialize(ENV, s.pop_size)) for s in SPECIES if not s.fixed}




        ## checking correctness
        for s, pop in pops.items():
           sm = sum(p.k for p in pop)
           assert sm == INTERACT_INDIVIDUALS_COUNT, \
               "For specie {0} count doesn't equal to {1}. Actual value {2}".format(s, INTERACT_INDIVIDUALS_COUNT, sm)

        print("Initialization finished")

        best = None
        for gen in range(GENERATIONS):


            # assign id for every elements in every population
            # create dictionary for all individuals in all pop
            i = 0
            for s, pop in pops.items():
                for p in pop:
                    p.id = i
                    i += 1
            ind_maps = {p.id: p for s, pop in pops.items() for p in pop}

            print("Gen: " + str(gen))
            ## constructing set of possible solutions
            solutions = []
            for i in range(INTERACT_INDIVIDUALS_COUNT):
                solution = DictBasedIndividual({s.name: decrease_k(choose(pop)) if not s.fixed
                                                else s.representative_individual
                                                for s, pop in pops.items()})
                solutions.append(solution)

            print("Solutions have been built")

            logsolutions(logbook, gen, solutions)

            ## estimate fitness
            for sol in solutions:
                sol.fitness = fitness(ENV, solution)

            print("Fitness have been evaluated")

            ## determine and distribute credits between participants of solutions
            ## TODO: perhaps it should be reconsidered
            inds_credit = dict()
            for sol in solutions:
                for s, ind in sol.items():
                    values = inds_credit.get(ind.id, [0, 0])
                    values[0] += sol.fitness / len(sol)
                    values[1] += 1
                    inds_credit[ind.id] = values
            for id, values in inds_credit.items():
                ## assign credit to every individual
                ind_maps[id].fitness = values[0] / values[1]

            print("Credit have been estimated")

            logpops(logbook, gen, pops)

            ## select best solution as a result
            ## TODO: remake it in a more generic way
            ## TODO: add archive and corresponding updating and mixing
            best = min(solutions, key=lambda x: x.fitness)

            ## produce offsprings
            items = [(s, pop) for s, pop in pops.items() if not s.fixed]
            for s, pop in items:
                offspring = s.select(ENV, pop)
                offspring = list(map(deepcopy, offspring))
                # Apply crossover and mutation on the offspring
                for child1, child2 in zip(offspring[::2], offspring[1::2]):
                    if random.random() < s.cxb:
                        c1 = child1.fitness
                        c2 = child2.fitness
                        s.mate(ENV, child1, child2)
                        ## TODO: make credit inheritance here
                        ## TODO: toolbox.inherit_credit(pop, child1, child2)
                        ## TODO: perhaps, this operation should be done after all crossovers in the pop
                        ## default implementation
                        child1.fitness = (c1 + c2) / 2
                        child2.fitness = (c1 + c2) / 2
                        pass
                    pass

                for mutant in offspring:
                    if random.random() < s.mb:
                        s.mutate(ENV, mutant)
                    pass
                pops[s] = offspring
                pass
            for s, pop in pops.items():
                credit_to_k(pop)
                for ind in pop:
                    del ind.fitness
                    del ind.id
            pass
            print("Offsprings have been generated")
        return best
    return func

def run_cooperative_ga(**kwargs):
    return create_cooperative_ga(**kwargs)()