from copy import deepcopy
import random
from deap import tools
from GA.DEAPGA.coevolution.cga import Specie
from core.HeftHelper import HeftHelper

"""
chromosome = [(Task(), Node()), ...]
"""
class Mapping(Specie):
    def __init__(self, name, pop_size, fixed=False, representative_individual=None):
        super().__init__(name, pop_size, fixed, representative_individual)
        pass

    @staticmethod
    def initialize(nodes, tasks, size):
        rnd = lambda: random.randint(0, len(nodes) - 1)
        result = [[(t, nodes[rnd()]) for t in tasks] for i in range(size)]
        return result

    @staticmethod
    def crossover(child1, child2):
        tools.cxOnePoint(child1, child2)

    @staticmethod
    def mutation(nodes, mutant):
        k = random.randint(0, len(mutant) - 1)
        (t, n) = mutant[k]
        mutant[k] = (t, nodes[random.randint(0, len(nodes) - 1)])
    pass

"""
chromosome = [Task(), Task(), ...]
"""
class Ordering(Specie):

    @staticmethod
    def has_parent_child_relation(task1, task2):
        pass

    def __init__(self, nodes, estimator, wf, name, pop_size, fixed=False, representative_individual=None):
        super().__init__(name, pop_size, fixed, representative_individual)
        self.nodes = nodes
        self.estimator = estimator
        self.wf = wf
        pass

    def initialize(self, size):
        ranking = HeftHelper.build_ranking_func(self.nodes, lambda job, agent: self.estimator.estimate_runtime(job, agent),
                                                           lambda ni, nj, A, B: self.estimator.estimate_transfer_time(A, B, ni, nj))
        sorted_tasks = ranking(self.wf)
        result = [self.mutation(deepcopy(sorted_tasks)) for i in range(size)]
        return result

    @staticmethod
    def crossover(child1, child2):
        def cutby(p1, p2, k):
            d = set(p1[0:k]) - set(p2[0:k])
            f = set(p1[0:k]) - set(p2[0:k])

            i = 0
            res = []
            for s in child2[0:k]:
                if s in d:
                    res.append(f[i])
                    i += 1
                else:
                    res.append(s)

            assert i == len(d), "There are unsetted elements"
            return p1[0:k] + res

        k = random.randint(1, len(child1) - 1)
        first = cutby(child1, child2, k)
        second = cutby(child2, child1, k)
        ## TODO: remake it
        child1.clear()
        child1.extend(first)
        child2.clear()
        child2.extend(second)
        pass

    def mutation(self, mutant):
        while True:
            k1 = random.randint(0, len(mutant) - 1)
            k2 = random.randint(0, len(mutant) - 1)
            if k1 != k2 and not self.has_parent_child_relation(mutant[k1], mutant[k2]):
                break
        mutant[k1], mutant[k2] = mutant[k2], mutant[k1]
        pass

"""
chromosome = [Config, NONE ,...]
"""
class ResourceConfig(Specie):
    def __init__(self, resource_manager, name, pop_size, fixed=False, representative_individual=None):
        super().__init__(name, pop_size, fixed, representative_individual)
        self.resource_manager = resource_manager
        pass

    def initialize(self, size):
        ## TODO: implement that methods
        raise NotImplementedError()
        slots = self.resource_manager.get_slots()
        configs = self.resource_manager.get_configs()
        genconf = lambda: random.randint(0, len(configs) - 1)
        result = [[genconf() if random.random() > 0.5 else None for s in slots] for i in range(size)]
        return result

    def crossover(self, child1, child2):
        tools.cxTwoPoint(child1, child2)
        pass

    def mutate(self, mutant):
        configs = self.resource_manager.get_configs()
        cfgs = configs + [None]
        k = random.randint(0, len(mutant) - 1)
        c = random.randint(0, len(cfgs) - 1)
        mutant[k] = cfgs[c]
        pass
    pass



