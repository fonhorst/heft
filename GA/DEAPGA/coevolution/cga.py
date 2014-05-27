from collections import namedtuple
from copy import deepcopy
import random

from deap import creator, tools
import numpy

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

creator.create("Individual", list)
ListBasedIndividual = creator.Individual


def rounddec(func):
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        res = int(res*100.0)/100.0
        return res
    return wrapper

def rounddeciter(func):
    def wrapper(*args, **kwargs):
        results = func(*args, **kwargs)
        res = [int(res*100)/100 for res in results]
        return res
    return wrapper

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
            self.name = kwargs["name"]
            self.pop_size = 1
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
        self.stat = kwargs.get("stat", lambda pop: {})
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

        solstat = kwargs.get("solstat", lambda sols: {})

        choose = kwargs["operators"]["choose"]
        fitness = kwargs["operators"]["fitness"]
        assign_credits = kwargs["operators"]["assign_credits"]

        USE_CREDIT_INHERITANCE = kwargs.get("use_credit_inheritance", False)

        # assert INTERACT_INDIVIDUALS_COUNT >= min(s.pop_size for s in SPECIES), \
        #     "Count of interacting individuals cannot be lower than size of any population." \
        #     " This restriction will be removed in future releases"


        stat = tools.Statistics(key=lambda x: x.fitness)
        stat.register("best", rounddec(numpy.max))
        stat.register("min", rounddec(numpy.min))
        stat.register("avg", rounddec(numpy.average))
        stat.register("std", rounddec(numpy.std))




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

        logbook = tools.Logbook()

        ## TODO: make processing of population consisting of 1 element uniform
        ## generate initial population
        pops = {s: generate_k(s.initialize(kwargs, s.pop_size)) if not s.fixed
        else generate_k([s.representative_individual])
                for s in SPECIES}
        ## make a copy for logging. TODO: remake it with logbook later.
        initial_pops = {s.name: deepcopy(pop) for s, pop in pops.items()}

        ## checking correctness
        for s, pop in pops.items():
           sm = sum(p.k for p in pop)
           assert sm == INTERACT_INDIVIDUALS_COUNT, \
               "For specie {0} count doesn't equal to {1}. Actual value {2}".format(s, INTERACT_INDIVIDUALS_COUNT, sm)

        print("Initialization finished")

        best = None
        for gen in range(GENERATIONS):

            print("Gen: " + str(gen))
            kwargs['gen'] = gen
            ## constructing set of possible solutions
            solutions = []
            for i in range(INTERACT_INDIVIDUALS_COUNT):
                solution = DictBasedIndividual({s.name: decrease_k(choose(kwargs, pop)) if not s.fixed
                                                else s.representative_individual
                                                for s, pop in pops.items()})
                solutions.append(solution)

            print("Solutions have been built")

            ## estimate fitness
            for sol in solutions:
                sol.fitness = fitness(kwargs, sol)

            print("Fitness have been evaluated")

            for s, pop in pops.items():
                for p in pop:
                    p.fitness = -1000000000.0

            ## assign id, calculate credits and save it
            i = 0
            for s, pop in pops.items():
                for p in pop:
                    p.id = i
                    i += 1
            ind_maps = {p.id: p for s, pop in pops.items() for p in pop}
            ind_to_credits = assign_credits(kwargs, solutions)
            for ind_id, credit in ind_to_credits.items():
                ## assign credit to every individual
                ind_maps[ind_id].fitness = credit

            assert all([sum(p.fitness for p in pop) != 0 for s, pop in pops.items()]), \
                "Error. Some population has individuals only with zero fitness"

            print("Credit have been estimated")

            #{s:distance.hamming() for s, pop in pops.items()}

            solsstat_dict = dict(list(stat.compile(solutions).items()) + list(solstat(solutions).items()))
            solsstat_dict["fitnesses"] = [sol.fitness for sol in solutions]

            popsstat_dict = {s.name: dict(list(stat.compile(pop).items()) + list(s.stat(pop).items())) for s, pop in pops.items()}
            for s, pop in pops.items():
                popsstat_dict[s.name]["fitnesses"] = [p.fitness for p in pop]

            logbook.record(gen=gen,
                           popsstat=(popsstat_dict,),
                           solsstat=(solsstat_dict,))

            #_logpops(logbook, gen, pops, solutions)

            ## select best solution as a result
            ## TODO: remake it in a more generic way
            ## TODO: add archive and corresponding updating and mixing
            best = max(solutions, key=lambda x: x.fitness)

            ## produce offsprings
            items = [(s, pop) for s, pop in pops.items() if not s.fixed]
            for s, pop in items:
                if s.fixed:
                    continue
                offspring = s.select(kwargs, pop)
                offspring = list(map(deepcopy, offspring))
                # Apply crossover and mutation on the offspring
                for child1, child2 in zip(offspring[::2], offspring[1::2]):
                    if random.random() < s.cxb:
                        c1 = child1.fitness
                        c2 = child2.fitness
                        s.mate(kwargs, child1, child2)
                        ## TODO: make credit inheritance here
                        ## TODO: toolbox.inherit_credit(pop, child1, child2)
                        ## TODO: perhaps, this operation should be done after all crossovers in the pop
                        ## default implementation
                        ## ?
                        child1.fitness = (c1 + c2) / 2.0
                        child2.fitness = (c1 + c2) / 2.0
                        pass
                    pass

                for mutant in offspring:
                    if random.random() < s.mb:
                        s.mutate(kwargs, mutant)
                    pass
                pops[s] = offspring
                pass

            ## assign credits for every individuals of all pops
            for s, pop in pops.items():
                credit_to_k(pop)

            for s, pop in pops.items():
                for ind in pop:
                    if not USE_CREDIT_INHERITANCE:
                        del ind.fitness
                    del ind.id
            pass
            print("Offsprings have been generated")
        return best, pops, logbook, initial_pops
    return func

def run_cooperative_ga(**kwargs):
    return create_cooperative_ga(**kwargs)()