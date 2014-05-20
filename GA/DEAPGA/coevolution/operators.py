from copy import deepcopy
from functools import partial
import random
from deap import tools
from deap import base
from deap import creator
from GA.DEAPGA.GAImplementation.NewSchedulerBuilder import place_task_to_schedule
from GA.DEAPGA.coevolution.cga import Specie, Env
from core.DSimpleHeft import DynamicHeft
from core.HeftHelper import HeftHelper
from core.concrete_realization import ExperimentResourceManager
from environment.ResourceGenerator import ResourceGenerator
from environment.ResourceManager import Schedule
from environment.Utility import Utility

creator.create("Individual", list)
ListBasedIndividual = creator.Individual

MAPPING_SPECIE = "MappingSpecie"
ORDERING_SPECIE = "OrderingSpecie"
RESOURCE_CONFIG_SPECIE = "ResourceConfigSpecie"

## TODO: move it to experiments/five_runs/

def default_choose(pop):
    while True:
        i = random.randint(0, len(pop) - 1)
        if pop[i].k > 0:
            return pop[i]


def _check_precedence(workflow, seq):
    for i in range(len(seq)):
        task = workflow.byId(seq[i])
        pids = [p.id for p in task.parents]
        for j in range(i + 1, len(seq)):
            if seq[j] in pids:
                return False
    return True


def build_schedule(workflow, estimator, resource_manager, solution):
    """
    the solution consists all parts necessary to build whole solution
    For the moment, it is mentioned that all species taking part in algorithm
    are necessary to build complete solution
    solution = {
        s1.name: val1,
        s2.name: val2,
        ....
    }
    """
    ms = solution[MAPPING_SPECIE]
    os = solution[ORDERING_SPECIE]

    assert _check_precedence(workflow, os), "Precedence is violated"

    ms = {t: resource_manager.byName(n) for t, n in ms}
    schedule_mapping = {n: [] for n in set(ms.values())}
    task_to_node = {}
    for t in os:
        node = ms[t]
        t = workflow.byId(t)
        (start_time, end_time) = place_task_to_schedule(workflow,
                                                        estimator,
                                                        schedule_mapping,
                                                        task_to_node,
                                                        ms, t, node, 0)

        task_to_node[t.id] = (node, start_time, end_time)
    schedule = Schedule(schedule_mapping)
    return schedule


def fitness_mapping_and_ordering(env,
                                 solution):
    schedule = build_schedule(env.wf, env.estimator, env.rm, solution)
    result = Utility.makespan(schedule)
    #result = ExecutorRunner.extract_result(schedule, True, workflow)
    return 1/result


## TODO: very simple version, As a ResourceConfig specie It will have to be extended to apply deeper analysis of situations
def fitness_ordering_resourceconf(workflow,
                                  estimator,
                                  solution):
    os = solution[ORDERING_SPECIE]
    rcs = solution[RESOURCE_CONFIG_SPECIE]
    ## TODO: refactor this
    flops_set = [conf.flops for conf in rcs if conf is not None]
    resources = ResourceGenerator.r(flops_set)
    resource_manager = ExperimentResourceManager(resources)
    heft = DynamicHeft(workflow, resource_manager, estimator, os)
    schedule = heft.run({n: [] for n in resource_manager.get_nodes()})
    result = Utility.makespan(schedule)
    return 1/result

##====================================
##Mapping specie
##====================================
"""
chromosome = [(task_id, node_name), ...]
"""


def mapping_default_initialize(env, size):
    nodes = list(env.rm.get_nodes())
    tasks = list(env.wf.get_all_unique_tasks())
    rnd = lambda: random.randint(0, len(nodes) - 1)
    result = [ListBasedIndividual((t.id, nodes[rnd()].name) for t in tasks)
              for i in range(size)]
    return result


def mapping_default_mutate(env, mutant):
    nodes = list(env.rm.get_nodes())
    k = random.randint(0, len(mutant) - 1)
    (t, n) = mutant[k]
    names = [node.name for node in nodes if node.name != n]
    mutant[k] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

##===================================
## Ordering specie
##==================================


def ordering_default_initialize(env, size):
    estimator = env.estimator
    nodes = env.rm.get_nodes()
    wf = env.wf
    ranking = HeftHelper.build_ranking_func(nodes,
                                            lambda job, agent: estimator.estimate_runtime(job, agent),
                                            lambda ni, nj, A, B: estimator.estimate_transfer_time(A, B, ni, nj))
    sorted_tasks = [t.id for t in ranking(wf)]

    assert _check_precedence(wf, sorted_tasks), "Check precedence failed"

    result = [ListBasedIndividual(ordering_default_mutate(env, deepcopy(sorted_tasks))) for i in range(size)]
    return result


def ordering_default_mutate(env, mutant):
    wf = env.wf
    while True:
        k1 = random.randint(0, len(mutant) - 1)
        k2 = random.randint(0, len(mutant) - 1)
        mx, mn = max(k1, k2), min(k1, k2)
        pids = [p.id for p in wf.byId(mutant[mx]).parents]
        cids = wf.ancestors(mutant[mn])
        if not any(el in pids or el in cids for el in mutant[mn: mx + 1]):
            break
    mutant[k1], mutant[k2] = mutant[k2], mutant[k1]
    #assert _check_precedence(self.wf, mutant), "Precedence is violated"
    return mutant


def ordering_default_crossover(env, child1, child2):
    def cutby(p1, p2, k):
        d = set(p1[0:k]) - set(p2[0:k])
        f = set(p2[0:k]) - set(p1[0:k])
        migr = [p for p in p2[0:k] if p in f]
        rest = [p for p in p2[k:] if p not in d]
        return p1[0:k] + migr + rest

    k = random.randint(1, len(child1) - 1)
    first = cutby(child1, child2, k)
    second = cutby(child2, child1, k)
    ## TODO: remake it
    child1.clear()
    child1.extend(first)
    child2.clear()
    child2.extend(second)
    pass

##===========================================
## ResourceConfig specie
##==========================================



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
        ## TODO: implement
        result = [ListBasedIndividual(genconf() if random.random() > 0.5 else None for s in slots)
                  for i in range(size)]
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


class VMConfig:
    def __init__(self, flops):
        self.flops = flops

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

##=====================================
## Default configs
##=====================================

def default_config(wf, rm, estimator):
    selector = lambda env, pop: tools.selTournament(pop, len(pop), 4)
    return {
        "interact_individuals_count": 22,
        "generations": 5,
        "env": Env(wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=10,
                           cxb=0.8, mb=0.5,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           mutate=mapping_default_mutate,
                           select=selector,
                           initialize=mapping_default_initialize
                    ),
                    Specie(name=ORDERING_SPECIE, pop_size=10,
                           cxb=0.8, mb=0.5,
                           mate=ordering_default_crossover,
                           mutate=ordering_default_mutate,
                           select=selector,
                           initialize=ordering_default_initialize,
                    )
        ],

        "operators": {
            "choose": default_choose,
            "fitness": fitness_mapping_and_ordering
        }
    }

# ## TODO: replace Task and node instances with dictionary(due to deepcopy)
# def create_simple_toolbox(workflow, estimator, resource_manager, **kwargs):
#     pop_size = kwargs["pop_size"]
#     interact_individuals_count = kwargs["interact_individuals_count"]
#     generations = kwargs["generations"]
#     mutation_probability = kwargs["mutation_probability"]
#     crossover_probability = kwargs["crossover_probability"]
#
#     toolbox = base.Toolbox()
#
#     MAPPING = Mapping(name="MappingSpecie", pop_size=pop_size, cxb=crossover_probability, mb=mutation_probability)
#     ORDERING = Ordering(name="OrderingSpecie", pop_size=pop_size, cxb=crossover_probability, mb=mutation_probability)
#
#     toolbox.species = [MAPPING, ORDERING]
#     toolbox.interact_individuals_count = interact_individuals_count
#     toolbox.generations = generations
#
#
#     init_op = {
#         MAPPING: partial(Mapping.default_initialize, resource_manager.get_nodes()),
#         ORDERING: partial(Ordering.default_initialize),
#     }
#
#     mate_op = {
#         MAPPING: tools.cxOnePoint,
#         ORDERING: Ordering.default_crossover,
#     }
#
#     mutate_op = {
#         MAPPING: partial(Mapping.default_mutate, resource_manager.get_nodes()),
#         ORDERING: partial(Ordering.default_mutate, workflow)
#     }
#
#     toolbox.register("initialize", lambda s, size: init_op[s](size))
#     toolbox.register("choose", default_choose)
#     toolbox.register("fitness", lambda s: fitness_mapping_and_ordering(workflow, estimator, resource_manager, s))
#     toolbox.register("select", lambda s, pop: tools.selTournament(pop, len(pop), 4))
#     toolbox.register("mate", lambda s, child1, child2: mate_op[s](child1, child2))
#     toolbox.register("mutate", lambda s, mutant: mutate_op[s](mutant))
#     return toolbox





