from collections import namedtuple
from copy import deepcopy
import random

from deap import creator, tools
from deap.tools import HallOfFame
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

creator.create("ListBasedIndividual", list)
ListBasedIndividual = creator.ListBasedIndividual


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

# class Specie:
#     def __init__(self, **kwargs):
#         self.name = kwargs["name"]
#         ## TODO: reconsider, should It be here?
#         self.pop_size = kwargs["pop_size"]
#         ## crossover probability
#         self.cxb = kwargs["cxb"]
#         ## mutation probability
#         self.mb = kwargs["mb"]
#
#         self.fixed = kwargs.get("fixed", None)
#         self.representative_individual = kwargs.get("representative_individual", None)
#     pass

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


class CoevolutionGA:

    def __init__(self, **kwargs):
        ## TODO: add ability to determine if evolution has stopped
        ## TODO: add archive
        ## TODO: add generating and collecting different statistics
        ## TODO: add logging
        # create initial populations of all species
        # taking part in coevolution
        self.kwargs = deepcopy(kwargs)
        self.ENV = kwargs["env"]
        self.SPECIES = kwargs["species"]
        self.INTERACT_INDIVIDUALS_COUNT = kwargs["interact_individuals_count"]
        self.GENERATIONS = kwargs["generations"]

        self.solstat = kwargs.get("solstat", lambda sols: {})
        #choose = kwargs["operators"]["choose"]
        self.build_solutions = kwargs["operators"]["build_solutions"]

        self.fitness = kwargs["operators"]["fitness"]
        self.assign_credits = kwargs["operators"]["assign_credits"]
        self.analyzers = kwargs.get("analyzers", [])
        self.USE_CREDIT_INHERITANCE = kwargs.get("use_credit_inheritance", False)
        self.HALL_OF_FAME_SIZE = kwargs.get("hall_of_fame_size", 0)

        self._result = None

        self.stat = tools.Statistics(key=lambda x: x.fitness)
        self.stat.register("best", rounddec(numpy.max))
        self.stat.register("min", rounddec(numpy.min))
        self.stat.register("avg", rounddec(numpy.average))
        self.stat.register("std", rounddec(numpy.std))

        self.logbook = tools.Logbook()
        self.kwargs['logbook'] = self.logbook

        ## TODO: make processing of population consisting of 1 element uniform
        ## generate initial population
        self.pops = {s: self._generate_k(s.initialize(self.kwargs, s.pop_size)) if not s.fixed
        else self._generate_k([s.representative_individual])
                for s in self.SPECIES}

        ## make a copy for logging. TODO: remake it with logbook later.
        self.initial_pops = {s.name: deepcopy(pop) for s, pop in self.pops.items()}

        ## checking correctness
        for s, pop in self.pops.items():
           sm = sum(p.k for p in pop)
           assert sm == self.INTERACT_INDIVIDUALS_COUNT, \
               "For specie {0} count doesn't equal to {1}. Actual value {2}".format(s, self.INTERACT_INDIVIDUALS_COUNT, sm)

        print("Initialization finished")

        self.hall = HallOfFame(self.HALL_OF_FAME_SIZE)

        self.kwargs['gen'] = 0

        pass

    def __call__(self):
        for gen in range(self.GENERATIONS):
            self.gen()
            print("Offsprings have been generated")
            pass
        return self.result()

    def statistic_processing(self, solutions):

        if len(solutions) > 0 :
            best_solution = solutions[0]
            for sol in solutions :
                if best_solution.fitness < sol.fitness:
                    best_solution = sol
            bf = str(best_solution.fitness)

            print("Fitness have been evaluated. Best is " + str(bf) + ' amoung ' + str(len(solutions)) + ' solutions')


    def gen(self):
        kwargs = self.kwargs
        kwargs['gen'] = kwargs['gen'] + 1
        print("Gen: " + str(kwargs['gen']))
        solutions = self.build_solutions(self.pops, self.INTERACT_INDIVIDUALS_COUNT)

        print("Solutions have been built")

        ## estimate fitness
        for sol in solutions:
            sol.fitness = self.fitness(kwargs, sol)

        self.statistic_processing(solutions)

        for s, pop in self.pops.items():
            for p in pop:
                p.fitness = -1000000000.0

        ## assign id, calculate credits and save it

        i = 0
        for s, pop in self.pops.items():
            for p in pop:
                p.id = i
                i += 1
        ind_maps = {p.id: p for s, pop in self.pops.items() for p in pop}
        ind_to_credits = self.assign_credits(kwargs, solutions)
        for ind_id, credit in ind_to_credits.items():
            ## assign credit to every individual
            ind_maps[ind_id].fitness = credit

        assert all([sum(p.fitness for p in pop) != 0 for s, pop in self.pops.items()]), \
                "Error. Some population has individuals only with zero fitness"

        print("Credit have been estimated")

        #print("Dict 1: " + str(len(list(self.stat.compile(solutions).items()))) + ' Dict2: ' + str(len(list(self.solstat(solutions).items()))))
        solsstat_dict = {}
        #solsstat_dict = dict(list(self.stat.compile(solutions).items()) + list(self.solstat(solutions).items()))
        solsstat_dict["fitnesses"] = [sol.fitness for sol in solutions]

        popsstat_dict = {s.name: dict(list(self.stat.compile(pop).items()) + list(s.stat(pop).items())) for s, pop in self.pops.items()}
        for s, pop in self.pops.items():
            popsstat_dict[s.name]["fitnesses"] = [p.fitness for p in pop]

        if self.hall.maxsize > 0:
            self.hall.update(solutions)
            ## TODO: this should be reconsidered
            lsols = len(solutions)
            solutions = list(deepcopy(self.hall)) + solutions
            solutions = solutions[0:lsols]

        self.logbook.record(gen=kwargs['gen'],
                        popsstat=(popsstat_dict,),
                        solsstat=(solsstat_dict,))

        for an in self.analyzers:
            an(kwargs, solutions, self.pops)

        print("hall: " + str(list(map(lambda x: x.fitness, self.hall))))

        ## select best solution as a result
        ## TODO: remake it in a more generic way
        ## TODO: add archive and corresponding updating and mixing
        #best = max(solutions, key=lambda x: x.fitness)

        ## take the best
        # best = hall[0] if hall.maxsize > 0 else max(solutions, key=lambda x: x.fitness)
        self.best = self.hall[0] if self.hall.maxsize > 0 else max(solutions, key=lambda x: x.fitness)

        #print("best selected.")
        ## produce offsprings
        items = [(s, pop) for s, pop in self.pops.items() if not s.fixed]
        for s, pop in items:
            #print("item pair operating.")
            if s.fixed:
                continue
            offspring = s.select(kwargs, pop)
            #print("     offspring selected.")
            offspring = list(map(deepcopy, offspring))

            ## apply mixin elite ones from the hall
            if self.hall.maxsize > 0:
                elite = [deepcopy(sol[s.name]) for sol in self.hall]
                offspring = elite + offspring
                offspring = offspring[0:len(pop)]


            # Apply crossover and mutation on the offspring
            for child1, child2 in zip(offspring[::2], offspring[1::2]):
                if random.random() < s.cxb:
                    c1 = child1.fitness
                    c2 = child2.fitness
                    s.mate(kwargs, child1, child2)
                    #print("     mutation done, child fintess : " + str((c1 + c2) / 2.0))
                    ## TODO: make credit inheritance here
                    ## TODO: toolbox.inherit_credit(pop, child1, child2)
                    ## TODO: perhaps, this operation should be done after all crossovers in the pop
                    ## default implementation
                    ## ?
                    child1.fitness = (c1 + c2) / 2.0
                    child2.fitness = (c1 + c2) / 2.0
                    pass

            for mutant in offspring:
                if random.random() < s.mb:
                    #print("     mutation in offspring started")
                    s.mutate(kwargs, mutant)
                pass
            self.pops[s] = offspring
            pass

            ## assign credits for every individuals of all pops
        for s, pop in self.pops.items():
            self._credit_to_k(pop)

        for s, pop in self.pops.items():
            for ind in pop:
                if not self.USE_CREDIT_INHERITANCE:
                    del ind.fitness
                del ind.id
        pass



    def result(self):
        return self.best, self.pops, self.logbook, self.initial_pops

    def _generate_k(self, pop):
        base_k = int(self.INTERACT_INDIVIDUALS_COUNT / len(pop))
        free_slots = self.INTERACT_INDIVIDUALS_COUNT % len(pop)
        for ind in pop:
            ind.k = base_k

        for slot in range(free_slots):
            i = random.randint(0, len(pop) - 1)
            pop[i].k += 1
        return pop

    def _credit_to_k(self, pop):
        norma = self.INTERACT_INDIVIDUALS_COUNT / sum(el.fitness for el in pop)
        for c in pop:
            c.k = int(c.fitness * norma)
        left_part = self.INTERACT_INDIVIDUALS_COUNT - sum(c.k for c in pop)
        sorted_pop = sorted(pop, key=lambda x: x.fitness, reverse=True)
        for i in range(left_part):
            sorted_pop[i].k += 1
        return pop

    pass

class VMCoevolutionGA():

    vm_series = []

    def __init__(self, **kwargs):
        ## TODO: add ability to determine if evolution has stopped
        ## TODO: add archive
        ## TODO: add generating and collecting different statistics
        ## TODO: add logging
        # create initial populations of all species
        # taking part in coevolution
        self.kwargs = deepcopy(kwargs)
        self.ENV = kwargs["env"]
        self.SPECIES = kwargs["species"]
        self.INTERACT_INDIVIDUALS_COUNT = kwargs["interact_individuals_count"]
        self.GENERATIONS = kwargs["generations"]

        self.solstat = kwargs.get("solstat", lambda sols: {})
        #choose = kwargs["operators"]["choose"]
        self.build_solutions = kwargs["operators"]["build_solutions"]

        self.fitness = kwargs["operators"]["fitness"]
        self.assign_credits = kwargs["operators"]["assign_credits"]
        self.analyzers = kwargs.get("analyzers", [])
        self.USE_CREDIT_INHERITANCE = kwargs.get("use_credit_inheritance", False)
        self.HALL_OF_FAME_SIZE = kwargs.get("hall_of_fame_size", 0)

        self._result = None

        self.stat = tools.Statistics(key=lambda x: x.fitness)
        self.stat.register("best", rounddec(numpy.max))
        self.stat.register("min", rounddec(numpy.min))
        self.stat.register("avg", rounddec(numpy.average))
        self.stat.register("std", rounddec(numpy.std))

        self.logbook = tools.Logbook()
        self.kwargs['logbook'] = self.logbook

        ## TODO: make processing of population consisting of 1 element uniform
        ## generate initial population
        self.pops = {s: self._generate_k(s.initialize(self.kwargs, s.pop_size)) if not s.fixed
        else self._generate_k([s.representative_individual])
                for s in self.SPECIES}

        ## make a copy for logging. TODO: remake it with logbook later.
        self.initial_pops = {s.name: deepcopy(pop) for s, pop in self.pops.items()}

        ## checking correctness
        for s, pop in self.pops.items():
           sm = sum(p.k for p in pop)
           assert sm == self.INTERACT_INDIVIDUALS_COUNT, \
               "For specie {0} count doesn't equal to {1}. Actual value {2}".format(s, self.INTERACT_INDIVIDUALS_COUNT, sm)

        print("Initialization finished")

        self.hall = HallOfFame(self.HALL_OF_FAME_SIZE)

        self.kwargs['gen'] = 0

        pass

    def __call__(self):
        for gen in range(self.GENERATIONS):
            self.gen()
            print("Offsprings have been generated")
            pass
        return self.result()

    def get_max_resource_number(self, ga_individual):
        current_max = ga_individual[0][1]
        for task in ga_individual:
            if task[1] > current_max:
                current_max = task[1]
        return current_max

    def statistic_processing(self, solutions):

        stat_amount = [0] * 40
        for p in solutions: stat_amount[self.get_max_resource_number(p['GASpecie'])] += 1
        str_res = ''
        for s in stat_amount:
            str_res = str_res + ' ' + str(s)
        self.vm_series.append(str_res)

        print('distribution by resource amount: ' + str_res)

        if len(solutions) > 0 :
            best_solution = solutions[0]
            for sol in solutions :
                if best_solution.fitness < sol.fitness:
                    best_solution = sol
            bf = str(best_solution.fitness)

            ga = best_solution['GASpecie']
            vm = best_solution['ResourceConfigSpecie']
            res_num = ''
            res_fl = ''
            res_ids = ''
            for ch in ga : res_num += str(ch[1])
            for nodes in vm : res_fl += ' ' +  str(nodes.flops)
            for nodes in vm : res_ids += ' ' +  str(nodes.id)
            print(' fl '+res_fl)
            print(' fl '+res_ids)
            print(' num '+res_num)
            print("Fitness have been evaluated. Best is " + str(bf) + ' amoung ' + str(len(solutions)) + ' solutions')

    def result(self):
        return self.best, self.pops, self.logbook, self.initial_pops, self.hall, self.vm_series

    def _generate_k(self, pop):
        base_k = int(self.INTERACT_INDIVIDUALS_COUNT / len(pop))
        free_slots = self.INTERACT_INDIVIDUALS_COUNT % len(pop)
        for ind in pop:
            ind.k = base_k

        for slot in range(free_slots):
            i = random.randint(0, len(pop) - 1)
            pop[i].k += 1
        return pop

    def _credit_to_k(self, pop):
        norma = self.INTERACT_INDIVIDUALS_COUNT / sum(el.fitness for el in pop)
        for c in pop:
            c.k = int(c.fitness * norma)
        left_part = self.INTERACT_INDIVIDUALS_COUNT - sum(c.k for c in pop)
        sorted_pop = sorted(pop, key=lambda x: x.fitness, reverse=True)
        for i in range(left_part):
            sorted_pop[i].k += 1
        return pop
    
    def gen(self):
        kwargs = self.kwargs
        kwargs['gen'] = kwargs['gen'] + 1
        print("Gen: " + str(kwargs['gen']))
        solutions = self.build_solutions(self.pops, self.INTERACT_INDIVIDUALS_COUNT)

        print("Solutions have been built")

        ## estimate fitness
        for sol in solutions:
            sol.fitness = self.fitness(kwargs, sol)

        self.statistic_processing(solutions)

        for s, pop in self.pops.items():
            for p in pop:
                p.fitness = -1000000000.0

        ## assign id, calculate credits and save it

        i = 0
        for s, pop in self.pops.items():
            for p in pop:
                p.id = i
                i += 1
        ind_maps = {p.id: p for s, pop in self.pops.items() for p in pop}
        ind_to_credits = self.assign_credits(kwargs, solutions)
        for ind_id, credit in ind_to_credits.items():
            ## assign credit to every individual
            ind_maps[ind_id].fitness = credit

        assert all([sum(p.fitness for p in pop) != 0 for s, pop in self.pops.items()]), \
                "Error. Some population has individuals only with zero fitness"

        print("Credit have been estimated")

        #print("Dict 1: " + str(len(list(self.stat.compile(solutions).items()))) + ' Dict2: ' + str(len(list(self.solstat(solutions).items()))))
        solsstat_dict = {}
        #solsstat_dict = dict(list(self.stat.compile(solutions).items()) + list(self.solstat(solutions).items()))
        solsstat_dict["fitnesses"] = [sol.fitness for sol in solutions]

        popsstat_dict = {s.name: dict(list(self.stat.compile(pop).items()) + list(s.stat(pop).items())) for s, pop in self.pops.items()}
        for s, pop in self.pops.items():
            popsstat_dict[s.name]["fitnesses"] = [p.fitness for p in pop]

        if self.hall.maxsize > 0:
            self.hall.update(solutions)
            ## TODO: this should be reconsidered
            lsols = len(solutions)
            solutions = list(deepcopy(self.hall)) + solutions
            solutions = solutions[0:lsols]

        self.logbook.record(gen=kwargs['gen'],
                        popsstat=(popsstat_dict,),
                        solsstat=(solsstat_dict,))

        for an in self.analyzers:
            an(kwargs, solutions, self.pops)

        print("hall: " + str(list(map(lambda x: x.fitness, self.hall))))

        ## select best solution as a result
        ## TODO: remake it in a more generic way
        ## TODO: add archive and corresponding updating and mixing
        #best = max(solutions, key=lambda x: x.fitness)

        ## take the best
        # best = hall[0] if hall.maxsize > 0 else max(solutions, key=lambda x: x.fitness)
        self.best = self.hall[0] if self.hall.maxsize > 0 else max(solutions, key=lambda x: x.fitness)

        #print("best selected.")
        ## produce offsprings
        items = [(s, pop) for s, pop in self.pops.items() if not s.fixed]
        for s, pop in items:
            #print("item pair operating.")
            if s.fixed:
                continue
            offspring = s.select(kwargs, pop)
            #print("     offspring selected.")
            offspring = list(map(deepcopy, offspring))

            ## apply mixin elite ones from the hall
            if self.hall.maxsize > 0:
                elite = [deepcopy(sol[s.name]) for sol in self.hall]
                offspring = elite + offspring
                offspring = offspring[0:len(pop)]


            # Apply crossover and mutation on the offspring
            for child1, child2 in zip(offspring[::2], offspring[1::2]):
                if random.random() < s.cxb:
                    c1 = child1.fitness
                    c2 = child2.fitness
                    s.mate(kwargs, child1, child2)
                    #print("     mutation done, child fintess : " + str((c1 + c2) / 2.0))
                    ## TODO: make credit inheritance here
                    ## TODO: toolbox.inherit_credit(pop, child1, child2)
                    ## TODO: perhaps, this operation should be done after all crossovers in the pop
                    ## default implementation
                    ## ?
                    child1.fitness = (c1 + c2) / 2.0
                    child2.fitness = (c1 + c2) / 2.0
                    pass

            for mutant in offspring:
                if random.random() < s.mb:
                    #print("     mutation in offspring started")
                    s.mutate(kwargs, mutant)
                pass
            self.pops[s] = offspring
            pass

            ## assign credits for every individuals of all pops
        for s, pop in self.pops.items():
            self._credit_to_k(pop)

        for s, pop in self.pops.items():
            for ind in pop:
                if not self.USE_CREDIT_INHERITANCE:
                    del ind.fitness
                del ind.id
        pass


def vm_run_cooperative_ga(**kwargs):
    kwargs = deepcopy(kwargs)
    cga = VMCoevolutionGA(**kwargs)
    VMCoevolutionGA.vm_series = []
    return cga()

def run_cooperative_ga(**kwargs):
    kwargs = deepcopy(kwargs)
    cga = CoevolutionGA(**kwargs)
    return cga()