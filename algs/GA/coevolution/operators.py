from copy import deepcopy
import random
from deap import tools
import numpy
from algs.ga.GAImplementation.NewSchedulerBuilder import place_task_to_schedule
from algs.ga.coevolution.cga import Specie, Env, ListBasedIndividual, DictBasedIndividual
from algs.heft.DSimpleHeft import DynamicHeft
from algs.heft.HeftHelper import HeftHelper
from core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from core.environment.ResourceManager import Schedule
from core.environment.Utility import Utility
from core.environment.ResourceGenerator import ResourceGenerator


MAPPING_SPECIE = "MappingSpecie"
ORDERING_SPECIE = "OrderingSpecie"
RESOURCE_CONFIG_SPECIE = "ResourceConfigSpecie"

## TODO: move it to experiments/five_runs/
## TODO: obsolete, remove it later.
def default_choose(ctx, pop):
    while True:
        i = random.randint(0, len(pop) - 1)
        if pop[i].k > 0:
            return pop[i]

def default_build_solutions(pops, interact_count):
    def decrease_k(ind):
        ind.k -= 1
        return ind
    def choose(pop):
        while True:
            i = random.randint(0, len(pop) - 1)
            if pop[i].k > 0:
                return pop[i]

    solutions = [DictBasedIndividual({s.name: decrease_k(choose(pop)) if not s.fixed
                                                else s.representative_individual
                                                for s, pop in pops.items()})
                                                for i in range(interact_count)]

    return solutions

def one_to_one_build_solutions(pops, interact_count):
    ls = [len(pop) for s, pop in pops.items()]
    assert numpy.std(ls) == 0, "Pops have different lengths: " + str(pops)
    assert ls[0] == interact_count, "Interact count doesn't equal to length of pops"
    elts = [[(s, p) for p in pop] for s, pop in pops.items()]
    solutions = [DictBasedIndividual({s.name: pop for s, pop in el}) for el in zip(*elts)]
    return solutions


def default_assign_credits(ctx, solutions):
    # assign id for every elements in every population
    # create dictionary for all individuals in all pop
    inds_credit = dict()
    for sol in solutions:
        for s, ind in sol.items():
            values = inds_credit.get(ind.id, [0, 0])
            values[0] += float(sol.fitness) / len(sol)
            values[1] += 1
            inds_credit[ind.id] = values

    result = {ind_id: float(all_fit) / float(count) for ind_id, (all_fit, count) in inds_credit.items()}
    return result


def bonus_assign_credits(ctx, solutions):
    mn = min(solutions, key=lambda x: x.fitness).fitness
    k = 0.1

    # assign id for every elements in every population
    # create dictionary for all individuals in all pop
    inds_credit = dict()
    for sol in solutions:
        for s, ind in sol.items():
            values = inds_credit.get(ind.id, [0, 0])
            values[0] += float((sol.fitness - mn)*k + sol.fitness) / len(sol)
            values[1] += 1
            inds_credit[ind.id] = values

    result = {ind_id: float(all_fit) / float(count) for ind_id, (all_fit, count) in inds_credit.items()}
    return result

def bonus2_assign_credits(ctx, solutions):
    mn = min(solutions, key=lambda x: x.fitness).fitness
    mx = max(solutions, key=lambda x: x.fitness).fitness
    k = 0.1

    # assign id for every elements in every population
    # create dictionary for all individuals in all pop
    inds_credit = dict()
    for sol in solutions:
        for s, ind in sol.items():
            values = inds_credit.get(ind.id, [0, 0])
            # values[0] += float((pow((sol.fitness - mn)/(mx - mn), 2) + k) * sol.fitness) / len(sol)
            values[0] += 0.1 * sol.fitness / len(sol)
            values[1] += 1
            inds_credit[ind.id] = values

    result = {ind_id: float(all_fit) / float(count) for ind_id, (all_fit, count) in inds_credit.items()}
    return result

def max_assign_credits(ctx, solutions):
    # assign id for every elements in every population
    # create dictionary for all individuals in all pop
    inds_credit = dict()
    for sol in solutions:
        for s, ind in sol.items():
            values = inds_credit.get(ind.id, [])
            values.append(sol.fitness)
            inds_credit[ind.id] = values

    result = {ind_id: max(all_fits) for ind_id, all_fits in inds_credit.items()}
    return result

def assign_from_transfer_overhead(ctx, solutions):
    result = max_assign_credits(ctx, solutions)
    result = {k: v if v != 0 else -1 for k, v in result.items()}
    return result


# def initialize_from_predefined(ctx, name, size):
#     pop = ctx[name]
#     assert len(pop) == size, "Size of predefined population doesn't match to required size: {0} vs {1}"\
#         .format(len(pop), size)
#     return pop


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


def fitness_mapping_and_ordering(ctx,
                                 solution):
    env = ctx['env']
    schedule = build_schedule(env.wf, env.estimator, env.rm, solution)
    result = Utility.makespan(schedule)
    #result = ExecutorRunner.extract_result(schedule, True, workflow)
    return -result

def overhead_fitness_mapping_and_ordering(ctx,
                                 solution):
    env = ctx['env']
    #schedule = build_schedule(env.wf, env.estimator, env.rm, solution)

    task_to_node = {t: n for t, n in solution[MAPPING_SPECIE]}
    unique_tasks = env.wf.get_all_unique_tasks()
    transfer_overheads = 0
    for task in unique_tasks:
        tnode = task_to_node[task.id]
        ## TODO: this is hack. See parametres
        transfer_overheads += sum([env.estimator.estimate_transfer_time(env.rm.byName(tnode), env.rm.byName(task_to_node[p.id]), task, p) for p in task.parents if p.id != env.wf.head_task.id])

    compute_overheads = sum([env.estimator.estimate_runtime(task, env.rm.byName(task_to_node[task.id])) for task in unique_tasks])

    #result = Utility.makespan(schedule)
    #result = ExecutorRunner.extract_result(schedule, True, workflow)
    return -(transfer_overheads + compute_overheads)



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


def mapping_default_initialize(ctx, size):
    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    tasks = sorted(list(env.wf.get_all_unique_tasks()),key=lambda x: x.id)
    rnd = lambda: random.randint(0, len(nodes) - 1)
    result = [ListBasedIndividual((t.id, nodes[rnd()].name) for t in tasks)
              for i in range(size)]
    return result

def mapping_heft_based_initialize(ctx, size, heft_mapping, count):
    result = [deepcopy(heft_mapping) for i in range(count)]
    result = result + mapping_default_initialize(ctx, size - count)
    return result


def mapping_default_mutate(ctx, mutant):
    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    k = random.randint(0, len(mutant) - 1)
    (t, n) = mutant[k]
    names = [node.name for node in nodes if node.name != n]
    mutant[k] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

def mapping_k_mutate(ctx, k, mutant):
    env = ctx['env']
    nodes = list(env.rm.get_nodes())

    count = random.randint(1, k)

    genset = set()
    for i in range(count):
        for i in range(50):
            m = random.randint(0, len(mutant) - 1)
            if m not in genset:
                genset.add(m)
                break
        pass
    for i in genset:
        (t, n) = mutant[i]
        names = [node.name for node in nodes if node.name != n]
        mutant[i] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

def mapping_all_mutate(ctx, mutant):
    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    for i in range(len(mutant)):
        if random.random() < 1/len(mutant):
            (t, n) = mutant[i]
            names = [node.name for node in nodes if node.name != n]
            mutant[i] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

def mapping_all_mutate_configurable(ctx, mutant, k):

    # gen_num = ctx['gen']
    # k = 5 if gen_num < 100 else 1

    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    for i in range(len(mutant)):
        if random.random() < k/len(mutant):
            (t, n) = mutant[i]
            names = [node.name for node in nodes if node.name != n]
            mutant[i] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

## TODO: only for debug of experiments. remove this enity later
def mapping_all_mutate_variable(ctx, mutant):

    gen_num = ctx['gen']
    k = 5 if gen_num < 100 else 1

    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    for i in range(len(mutant)):
        if random.random() < k/len(mutant):
            (t, n) = mutant[i]
            names = [node.name for node in nodes if node.name != n]
            mutant[i] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

def mapping_all_mutate_variable2(ctx, mutant):

    gen_num = ctx['gen']
    if gen_num < 100:
        k = 20
    elif gen_num < 200:
        k = 10
    elif gen_num < 300:
        k = 5
    else:
        k = 1

    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    for i in range(len(mutant)):
        if random.random() < k/len(mutant):
            (t, n) = mutant[i]
            names = [node.name for node in nodes if node.name != n]
            mutant[i] = (t, nodes[random.randint(0, len(names) - 1)].name)
    pass

def mapping_improving_mutation(ctx, mutant):
    env = ctx["env"]
    task_to_node = {t: n for t, n in mutant}

    def estimate_overheads(task_id, node_name):
        task = env.wf.byId(task_id)
        node = env.rm.byName(node_name)
        ttime = env.estimator.estimate_transfer_time
        ptransfer_time = [ttime(node, env.rm.byName(task_to_node[p.id]), task, p) for p in task.parents if p != env.wf.head_task]
        ctransfer_time = [ttime(node, env.rm.byName(task_to_node[p.id]), task, p) for p in task.children]
        computation_time = env.estimator.estimate_runtime(task, node)
        return (ptransfer_time, ctransfer_time, computation_time)

    overheads = {t: estimate_overheads(t, n) for t,n in task_to_node.items()}
    sorted_overheads = sorted(overheads.items(), key=lambda x: x[1][0] + x[1][1])

    ## choose overhead for improving
    # try to improve max transfer overhead
    # t, oheads = sorted_overheads[0]
    for i in range(50):
        t, oheads = sorted_overheads[random.randint(0, int(len(sorted_overheads)/2))]

        # improving
        nodes = env.rm.get_nodes()
        potential_overheads = [(n, estimate_overheads(t, n.name)) for n in nodes if task_to_node[t] != n.name]

        n, noheads = min(potential_overheads, key=lambda x: x[1][0] + x[1][1])
        if oheads[0] + oheads[1] > noheads[0] + noheads[1]:
            for i in range(len(mutant)):
                t1, n1 = mutant[i]
                if t1 == t:
                    mutant[i] = (t, n.name)
                    break
                pass
    pass


class MutRegulator:
    def __init__(self, gens_count=30):
        self.generations = []
        self.gens_count = gens_count
        pass

    def __call__(self, mutation):
        def wrapper(ctx, mutant):
            if len(self.generations) >= self.gens_count and numpy.std(self.generations[-30::]) == 0:
                mutation(ctx, mutant, 2)
            else:
                mutation(ctx, mutant, 1)
        return wrapper

    def analyze(self, ctx, solutions, pops):
        gen = ctx['gen']
        best = max(solutions, key=lambda x: x.fitness)
        self.generations.append((gen, best.fitness))
        pass


class MappingArchiveMutate:
    def __init__(self):
        self._archive = set()
        pass

    def __call__(self, ctx, mutant):
        #for i in range(50):
        while True:
            # mt = deepcopy(mutant)
            mapping_default_mutate(ctx, mutant)
            h = hash(tuple(mutant))
            if h not in self._archive:
                self._archive.add(h)
                # for i in range(len(mutant)):
                #     mutant[i] = mt[i]
                break
        pass
    pass



##===================================
## Ordering specie
##==================================

def ordering_heft_based_initialize(ctx, size, heft_ordering, count):
    result = [deepcopy(heft_ordering) for i in range(count)]
    result = result + ordering_default_initialize(ctx, size - count)
    return result


def ordering_default_initialize(ctx, size):
    env = ctx['env']
    estimator = env.estimator
    nodes = env.rm.get_nodes()
    wf = env.wf
    ranking = HeftHelper.build_ranking_func(nodes,
                                            lambda job, agent: estimator.estimate_runtime(job, agent),
                                            lambda ni, nj, A, B: estimator.estimate_transfer_time(A, B, ni, nj))
    sorted_tasks = [t.id for t in ranking(wf)]

    assert _check_precedence(wf, sorted_tasks), "Check precedence failed"

    result = [ListBasedIndividual(ordering_default_mutate(ctx, deepcopy(sorted_tasks))) for i in range(size)]
    return result


def ordering_default_mutate(ctx, mutant):
    env = ctx['env']
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


def ordering_default_crossover(ctx, child1, child2):
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
            # "choose": default_choose,
            "build_solutions": default_build_solutions,
            "fitness": fitness_mapping_and_ordering,
            "assign_credits": default_assign_credits
        }
    }







