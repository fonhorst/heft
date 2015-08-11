from collections import namedtuple
from copy import deepcopy
import random
from deap import creator, tools
from deap.tools import HallOfFame
from heft.core.environment.BaseElements import Node
import numpy
from heft.algs.common.utilities import gather_info
from heft.algs.common.individuals import DictBasedIndividual, ListBasedIndividual

SPECIES = "species"
OPERATORS = "operators"
DEFAULT = "default"

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

"""
Toolbox need to implement functions:

"""

Env = namedtuple('Env', ['wf', 'rm', 'estimator'])

## to be able to pickle
def _empty_stat(pop):
    return {}


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
        self.stat = kwargs.get("stat", _empty_stat)
        pass

class VMCoevolutionGA():

    vm_series = []

    def __init__(self, **kwargs):
        self.kwargs = deepcopy(kwargs)
        self.ENV = kwargs["env"]
        self.CEMETERY = self.create_cemetery(self.kwargs)
        self.kwargs["cemetery"] = self.CEMETERY
        self.SPECIES = kwargs["species"]
        self.INTERACT_INDIVIDUALS_COUNT = kwargs["interact_individuals_count"]
        self.GENERATIONS = kwargs["generations"]

        self.solstat = kwargs.get("solstat", lambda sols: {})
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

        #self.nodes_cemetery = create_node_cemetery()

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

        #print("Initialization finished")

        self.hall = HallOfFame(self.HALL_OF_FAME_SIZE)

        self.kwargs['gen'] = 0

        pass

    def __call__(self):
        for gen in range(self.GENERATIONS):
            self.gen()
            pass
        return self.result()

    def create_cemetery(self, ctx):
        resources = self.ENV[1].resources
        cemetery = set()
        for res in resources:
            nodes_to_remove = []
            for node in res.nodes:
                if node.state == 'down':
                    cemetery.add(node)
                    nodes_to_remove.append(node)
            for node in nodes_to_remove:
                res.nodes.remove(node)
        for res in ctx["env"][1].resources:
            nodes_to_remove = []
            for node in res.nodes:
                if node.state == 'down':
                    nodes_to_remove.append(node)
            for node in nodes_to_remove:
                res.nodes.remove(node)
        return cemetery

    def get_max_resource_number(self, ga_individual):
        # returns maximal used resources index
        current_max = ga_individual[0][1]
        for task in ga_individual:
            if task[1] > current_max:
                current_max = task[1]
        return current_max

    def get_max_blade_number(self, ga_individual, res_idx):
        # return maximal used blade index in specified resource
        ga_ind_sub = [task for task in ga_individual if task[1] == res_idx]
        if len(ga_ind_sub) == 0:
            return -1
        current_max = ga_ind_sub[0][2]
        for task in ga_ind_sub:
            if task[2] > current_max:
                current_max = task[2]
        return current_max

    def statistic_processing(self, solutions):

        # TODO convert to list with distributions for each blade
        max_res_idx = self.get_max_resource_number(solutions[0]['GASpecie'])
        vm_stat_res = []
        for i in range(max_res_idx + 1):
            stat_amount = [0] * 40
            for p in solutions:
                cur_idx = self.get_max_blade_number(p['GASpecie'], i)
                if cur_idx == -1:
                    # TODO check this branch
                    continue
                stat_amount[cur_idx] += 1
            str_res = ''
            for s in stat_amount:
                str_res = str_res + ' ' + str(s)
            self.vm_series.append(str_res)
            vm_stat_res.append(str_res)

        #print('distribution by resource amount: ' + str(vm_stat_res))

        if len(solutions) > 0:
            best_solution = solutions[0]
            for sol in solutions:
                if best_solution.fitness < sol.fitness:
                    best_solution = sol
            bf = str(best_solution.fitness)

            ga = best_solution['GASpecie']
            vm = best_solution['ResourceConfigSpecie']
            res_num = ''
            res_fl = ''
            res_ids = ''
            for ch in ga:
                res_num += '(' + str(ch[1]) + ':' + str(ch[2]) + ')'
            for nodes in vm:
                res_fl += '('
                for node in nodes:
                    res_fl += ' ' + str(node.flops)
                res_fl += ')'
            for nodes in vm:
                res_ids += '('
                for node in nodes:
                    res_ids += ' ' + str(node.name)
                res_ids += ')'
            #print(' fl '+res_fl)
            #print(' fl '+res_ids)
            #print(' num '+res_num)
            #print("Fitness have been evaluated. Best is " + str(bf) + ' amoung ' + str(len(solutions)) + ' solutions')

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

        solutions = self.build_solutions(self.pops, self.INTERACT_INDIVIDUALS_COUNT)


        ## estimate fitness
        for sol in solutions:
            sol.fitness = self.fitness(kwargs, sol)

        # TODO add statistic
        #self.statistic_processing(solutions)

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

        #print("Credit have been estimated")

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

        stats = tools.Statistics(lambda ind: -ind.fitness)
        stats.register("avg", numpy.mean)
        stats.register("std", numpy.std)
        stats.register("min", numpy.min)
        stats.register("max", numpy.max)
        IS_SILENT = True

        gather_info(self.logbook, stats, kwargs['gen'] - 1, solutions + self.hall.items, None, need_to_print=not IS_SILENT)

        for an in self.analyzers:
            an(kwargs, solutions, self.pops)

        #print(str(kwargs['gen']) + " hall: mean = " + str(numpy.mean(list(map(lambda x: x.fitness, self.hall)))) + " max = " + str(numpy.max(list(map(lambda x: x.fitness, self.hall)))) + " min = " + str(numpy.min(list(map(lambda x: x.fitness, self.hall)))))

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
                    #print("cross prev")
                    #if s.name == 'ResourceConfigSpecie':
                    #    print("    child1 = " + str([(node, node.flops) for node in child1[0].nodes]))
                    #    print("    child2 = " + str([(node, node.flops) for node in child2[0].nodes]))
                    #else:
                    #    print("    child1 = " + str(child1))
                    #    print("    child2 = " + str(child2))
                    #print("     cross started")
                    if s.name == 'ResourceConfigSpecie':
                        chd1, chd2 = s.mate(kwargs, child1, child2)
                        chd1.fitness = (c1 + c2) / 2.0
                        chd2.fitness = (c1 + c2) / 2.0
                        pop.append(chd1)
                        pop.append(chd2)
                    else:
                        chd1, chd2 = s.mate(kwargs, child1, child2)
                        chd1.fitness = (c1 + c2) / 2.0
                        chd2.fitness = (c1 + c2) / 2.0
                        pop.append(chd1)
                        pop.append(chd2)
                    #print("cross after")
                    #if s.name == 'ResourceConfigSpecie':
                    #    print("    child1 = " + str([(node, node.flops) for node in chd1[0].nodes]))
                    #    print("    child2 = " + str([(node, node.flops) for node in chd2[0].nodes]))
                    #else:
                    #    print("    child1 = " + str(chd1))
                    #    print("    child2 = " + str(chd2))
                    #print("-----")
                    #print("     cross done, child fintess : " + str((c1 + c2) / 2.0))
                    ## TODO: make credit inheritance here
                    ## TODO: toolbox.inherit_credit(pop, child1, child2)
                    ## TODO: perhaps, this operation should be done after all crossovers in the pop
                    ## default implementation
                    ## ?

                    pass

            for mutant in offspring:
                if random.random() < s.mb:
                    #print("mutation started")
                    #print("mut prev")
                    #if s.name == 'ResourceConfigSpecie':
                    #    print("    mutant = " + str([(node, node.flops)for node in mutant[0].nodes]))
                    #else:
                    #    print("    mutant = " + str(mutant))
                    if s.name == 'ResourceConfigSpecie':
                        s.mutate(kwargs, mutant)
                    else:
                        s.mutate(kwargs, mutant)
                    #print("mut after")
                    #if s.name == 'ResourceConfigSpecie':
                    #    print("    mutant = " + str([(node, node.flops)for node in mutant[0].nodes]))
                    #else:
                    #    print("    mutant = " + str(mutant))
                    #print("----")
                    #print("mutation done")
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
    kwargs_copy = deepcopy(kwargs)
    cga = VMCoevolutionGA(**kwargs)
    VMCoevolutionGA.vm_series = []
    res = cga()
    res_rm = res[0]['ResourceConfigSpecie']
    kw_rm = kwargs_copy['env'][1].resources

    best = res[0]
    if isinstance(best['ResourceConfigSpecie'][0].nodes, set):
        raise Exception("Alarm! Debug")

    for (i, resource) in enumerate(res_rm):
        new_set = []
        kw_nodes = kw_rm[i].nodes
        names_of_alive_nodes = set(res_node.name for res_node in resource.nodes)
        for node in kw_nodes:
            if node.name not in names_of_alive_nodes:
                node.state = Node.Down
                new_set.append(node)
        for node in resource.nodes:
            if node.name not in [s_node.name for s_node in new_set]:
                new_set.append(node)
        for node in [c_node for c_node in cga.CEMETERY if c_node.resource.name == resource.name]:
            if node.name not in [s_node.name for s_node in new_set]:
                new_set.append(node)
        # TODO make a sepecial method in ResourceManager to change resource and node sets in a TRACKABLE way
        kwargs['env'][1].resources[i].nodes = new_set
    kwargs['cemetery'] = cga.CEMETERY


    return res