from copy import deepcopy
import random
from deap import tools
from deap import base
from GA.DEAPGA.GAImplementation.NewSchedulerBuilder import place_task_to_schedule
from GA.DEAPGA.coevolution.cga import Specie
from core.DSimpleHeft import DynamicHeft
from core.HeftHelper import HeftHelper
from core.concrete_realization import ExperimentResourceManager
from environment.Resource import ResourceGenerator
from environment.ResourceManager import Schedule
from environment.Utility import Utility


class VMConfig:
    def __init__(self, flops):
        self.flops = flops
    pass

def default_choose(pop):
    while True:
        i = random.randint(0, len(pop) - 1)
        if pop[i].k > 0:
            pop[i].k -= 1
            return pop[i]

"""
the solution consists all parts necessary to build whole solution
For the moment, it is mentioned that all species taking part in algorithm
are necessary to build complete solution
solution = {
    s1: val1,
    s2: val2,
    ....
}
"""
def fitness_mapping_and_ordering(workflow,
                                 estimator,
                                 solution):
    ms = solution[filter(lambda k: isinstance(k, Mapping), solution)]
    os = solution[filter(lambda k: isinstance(k, Ordering), solution)]


    ms = {t: n for t, n in ms}
    ## TODO: implement procedure for building schedule
    schedule_mapping = {n: [] for n in set(ms.values())}
    task_to_node = {}
    for t in os:
        node = ms[t]
        (start_time, end_time) = place_task_to_schedule(workflow,
                                                        estimator,
                                                        schedule_mapping,
                                                        task_to_node,
                                                        ms, t, node, 0)
        task_to_node[t.id] = (node, start_time, end_time)
    schedule = Schedule(schedule_mapping)
    result = Utility.makespan(schedule)
    return result

## TODO: very simple version, As a ResourceConfig specie It will have to be extended to apply deeper analysis of situations
def fitness_ordering_resourceconf(workflow,
                                  estimator,
                                  solution):
    os = solution[filter(lambda k: isinstance(k, Ordering), solution)]
    rcs = solution[filter(lambda k: isinstance(k, ResourceConfig), solution)]
    ## TODO: refactor this
    flops_set = [conf.flops for conf in rcs if conf is not None]
    resources = ResourceGenerator.r(flops_set)
    resource_manager = ExperimentResourceManager(resources)
    heft = DynamicHeft(workflow, resource_manager, estimator, os)
    schedule = heft.run({n: [] for n in resource_manager.get_nodes()})
    result = Utility.makespan(schedule)
    return result

class Mapping(Specie):
    """
    chromosome = [(Task(), Node()), ...]
    """
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

class Ordering(Specie):
    """
    chromosome = [Task(), Task(), ...]
    """
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

    """
    TODO: crossover can violate parent-child relations,
    so it will have to be reconsidered - solved, need to be checked
    """
    @staticmethod
    def crossover(child1, child2):
        def cutby(p1, p2, k):
            d = set(p1[0:k]) - set(p2[0:k])
            f = set(p1[0:k]) - set(p2[0:k])
            rest = set(p2[k:]) - f
            return p1[0:k] + list(f) + list(rest)

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

class ResourceConfig(Specie):
    """
    chromosome = [Config, NONE ,...]
    """
    def __init__(self, resource_manager, name, pop_size, fixed=False, representative_individual=None):
        super().__init__(name, pop_size, fixed, representative_individual)
        self.resource_manager = resource_manager
        pass

    def initialize(self, size):
        ## TODO: implement that methods
        raise NotImplementedError()
        slots = self.resource_manager.slots_count
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

class VMResourceManager(ExperimentResourceManager):
    def __init__(self, slots_count, resources=[]):
        super().__init__(resources)
        self.slots_count = slots_count

    def get_configs(self):
        SMALL = VMConfig(10)
        MEDIUM = VMConfig(30)
        HIGH = VMConfig(50)
        return [SMALL, MEDIUM, HIGH]
    pass

## TODO: replace Task and node instances with dictionary(due to deepcopy)

def create_simple_toolbox(workflow, estimator, resource_manager, **kwargs):
    pop_size = kwargs["pop_size"]
    interact_individuals_count = kwargs["interact_individuals_count"]
    generations = kwargs["generations"]
    mutation_probability = kwargs["mutation_probability"]
    crossover_probability = kwargs["crossover_probability"]

    toolbox = base.Toolbox()

    MAPPING = Mapping("MappingSpecie", pop_size)
    ORDERING = Ordering("OrderingSpecie", resource_manager.get_nodes(), estimator, workflow)

    toolbox.species = [MAPPING, ORDERING]
    toolbox.interact_individuals_count = interact_individuals_count
    toolbox.generations = generations
    MUTATION = {
        MAPPING: mutation_probability,
        ORDERING: mutation_probability,
    }

    CXB = {
        MAPPING: crossover_probability,
        ORDERING: crossover_probability,
    }


    operators_impl = {
        MAPPING: MAPPING,
        ORDERING: ORDERING,
    }
    toolbox.register("initialize", lambda s, size: operators_impl[s].initialize(size))
    toolbox.register("choose", default_choose)
    toolbox.register("fitness", fitness_mapping_and_ordering)
    toolbox.register("select", tools.selTournament, 4)
    toolbox.register("mate", lambda s, child1, child2: operators_impl[s].crossover(child1, child2))
    toolbox.register("mutate", lambda s, mutant: operators_impl[s].mutant)
    return toolbox




