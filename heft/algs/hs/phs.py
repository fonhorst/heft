from collections import namedtuple
from copy import deepcopy
import random
from deap import creator, tools
from heft.core.environment.ResourceManager import Schedule
import numpy
from heft.algs.common.individuals import DictBasedIndividual, ListBasedIndividual
from heft.algs.common.utilities import gather_info

SPECIES = "species"
OPERATORS = "operators"
DEFAULT = "default"

GA_SPECIE = "GASpecie"
RESOURCE_CONFIG_SPECIE = "ResourceConfigSpecie"

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

Env = namedtuple('Env', ['wf', 'rm', 'estimator'])

class Specie:
    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.pop_size = kwargs["pop_size"]
        self.mate = kwargs["mate"]
        self.mutate = kwargs["mutate"]
        self.initialize = kwargs["initialize"]
        # self.stat = kwargs.get("stat", _empty_stat)

class VMCoevolutionGA():

    vm_series = []

    def __init__(self, **kwargs):
        self.kwargs = deepcopy(kwargs)
        self.ENV = kwargs["env"]
        self.SPECIES = kwargs["species"]
        self.GENERATIONS = kwargs["generations"]

        self.fitness = kwargs["operators"]["fitness"]
        self._result = None
        self.stat = tools.Statistics(key=lambda x: x.fitness)
        self.stat.register("best", rounddec(numpy.max))
        self.stat.register("min", rounddec(numpy.min))
        self.stat.register("avg", rounddec(numpy.average))
        self.stat.register("std", rounddec(numpy.std))

        self.logbook = tools.Logbook()
        self.kwargs['logbook'] = self.logbook
        self.kwargs['gen'] = 0
        sched_ex = [(item[0], val) for item in kwargs['initial_schedule'].mapping.items() for val in item[1]]
        sched_ex.sort(key=lambda x: x[1].start_time)
        sched_ex = ListBasedIndividual((item[1].job.id, int(item[0].name[-1])) for item in sched_ex)
        res_ex = ListBasedIndividual(self.ENV[1].resources)
        res_ex = [node.flops for node in res_ex[0].nodes]
        # res_ex.sort(reverse=True)
        self.kwargs["fixed_schedule"] = Schedule({node: [] for node in kwargs['initial_schedule'].mapping})
        self.pops = {s: s.initialize(self.kwargs, s.pop_size - 1) for s in self.SPECIES}
        self.best = None
        for s in self.SPECIES:
            if s.name == "ResourceConfigSpecie":
                self.pops[s].append(ListBasedIndividual(res_ex))
                for rc in self.pops[s]:
                    sched_copy = deepcopy(sched_ex)
                    rc.fitness = self.fitness(self.kwargs, (sched_copy, rc))
                    if self.best is None or self.best.fitness < rc.fitness:
                        self.best = DictBasedIndividual({RESOURCE_CONFIG_SPECIE: rc, GA_SPECIE: sched_copy})
                        self.best.fitness = rc.fitness
            else:
                self.pops[s].append(sched_ex)
                for sc in self.pops[s]:
                    sc.fitness = self.fitness(self.kwargs, (sc, res_ex))
                    if self.best is None or self.best.fitness < sc.fitness:
                        self.best = DictBasedIndividual({RESOURCE_CONFIG_SPECIE: res_ex, GA_SPECIE: sc})
                        self.best.fitness = sc.fitness

        self.initial_pops = {s.name: deepcopy(pop) for s, pop in self.pops.items()}

        pass

    def __call__(self):
        for gen in range(self.GENERATIONS):
            self.gen()
        return self.result()

    def result(self):
        return self.best, self.pops, self.logbook, self.initial_pops, self.vm_series

    def gen(self):
        kwargs = self.kwargs
        kwargs['gen'] = kwargs['gen'] + 1

        sched_pop = None
        res_pop = None
        for s in self.pops:
            if s.name == GA_SPECIE:
                sched_pop = self.pops[s]
                scheds = s
            else:
                res_pop = self.pops[s]
                resources = s

        # sched_pop.sort(key=lambda x: x.fitness)
        # res_pop.sort(key=lambda x: x.fitness)

        # print("SCHED" + str(len(sched_pop)))
        # for sched in sched_pop:
        #     print(sched)
        #
        # print("RES" + str(len(res_pop)))
        # for res in res_pop:
        #     print(res)

        fitness_spop = [p.fitness for p in sched_pop]
        min_sfit = min(fitness_spop)
        fitness_rpop = [p.fitness for p in res_pop]
        min_rfit = min(fitness_rpop)

        p1 = 0.01
        p2 = 0.7
        if random.random() > 0.5:
            # Choose schedule
            sr_example = scheds.initialize(self.kwargs, 1)[0]
            sp_example = deepcopy(sched_pop[random.randint(0, len(sched_pop) - 1)])

            strat = random.random()
            if strat <= p1:
                s_example = sr_example
            if p1 < strat <= p2:
                s_example = sp_example
            if strat > p2:
                s_example = scheds.mate(kwargs, sr_example, sp_example)

            # selection
            res_copy = deepcopy(random.choice(res_pop))
            for imp in range(5):
                scheds.mutate(kwargs, s_example)

                # new_melody = DictBasedIndividual({RESOURCE_CONFIG_SPECIE: self.best[RESOURCE_CONFIG_SPECIE], GA_SPECIE: s_example})
                new_melody = DictBasedIndividual({RESOURCE_CONFIG_SPECIE: res_copy, GA_SPECIE: s_example})
                # new_melody = DictBasedIndividual({RESOURCE_CONFIG_SPECIE: res, GA_SPECIE: s_example})
                new_melody.fitness = self.fitness(kwargs, (s_example, res_copy))
                if new_melody.fitness > self.best.fitness:
                    self.best = new_melody
                    break
            s_example.fitness = new_melody.fitness

            # update scheds
            need_update = False
            if s_example.fitness > min_sfit and s_example.fitness not in fitness_spop:
                sched_pop.append(s_example)
                need_update = True
            if need_update:
                sorted_pop = sorted(sched_pop, key=lambda x: x.fitness, reverse=True)
                self.pops[scheds] = sorted_pop[:scheds.pop_size]

        else:
            # Choose resource
            rr_example = resources.initialize(self.kwargs, 1)[0]
            rp_example = deepcopy(res_pop[random.randint(0, len(res_pop) - 1)])

            p1 = 0.01
            p2 = 0.9

            strat = random.random()
            if strat <= p1:
                r_example = rr_example
            if p1 < strat <= p2:
                r_example = rp_example
            if strat > p2:
                r_example = resources.mate(kwargs, rr_example, rp_example)

            # selection
            best_copy = deepcopy(random.choice(sched_pop))
            for imp in range(3):
                resources.mutate(kwargs, r_example)
                # best_copy = deepcopy(self.best[GA_SPECIE])

                new_melody = DictBasedIndividual({RESOURCE_CONFIG_SPECIE: r_example, GA_SPECIE: best_copy})
                new_melody.fitness = self.fitness(kwargs, (best_copy, r_example))
                if new_melody.fitness > self.best.fitness:
                    self.best = new_melody
                    break
            r_example.fitness = new_melody.fitness

            # update resources
            need_update = False
            if r_example.fitness > min_rfit and r_example.fitness not in fitness_rpop:
                res_pop.append(r_example)
                need_update = True
            if need_update:
                sorted_pop = sorted(res_pop, key=lambda x: x.fitness, reverse=True)
                self.pops[resources] = sorted_pop[:resources.pop_size]

        stats = tools.Statistics(lambda ind: -ind.fitness)
        stats.register("avg", numpy.mean)
        stats.register("std", numpy.std)
        stats.register("min", numpy.min)
        stats.register("max", numpy.max)
        IS_SILENT = True

        solutions = self.pops[scheds] + self.pops[resources]
        gather_info(self.logbook, stats, kwargs['gen'] - 1, solutions, None, need_to_print=not IS_SILENT)

        # print("gen " + str(kwargs['gen']) + " Best = " + str(-self.best.fitness))
        fitness_rpop.sort(reverse=True)
        # print("min = " + str(numpy.min(fitness_rpop + fitness_spop)) + " mean = " + str(numpy.mean(fitness_rpop + fitness_spop)) + " max =  " + str(numpy.max(fitness_rpop + fitness_spop)))
        pass


def vm_run_cooperative_ga(**kwargs):
    cga = VMCoevolutionGA(**kwargs)
    VMCoevolutionGA.vm_series = []
    res = cga()
    return res
