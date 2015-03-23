from copy import deepcopy, copy
import random

from deap import tools
import numpy
from numpy.lib.function_base import append, insert
from heft.algs.common.NewSchedulerBuilder import place_task_to_schedule
from heft.algs.common.individuals import DictBasedIndividual, ListBasedIndividual
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE, check_precedence
from heft.algs.ga.coevolution.cga import Specie, Env
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import Utility
from heft.core.environment.ResourceGenerator import ResourceGenerator
from heft.core.environment.ResourceGenerator import ResourceGenerator
from heft.core.environment.BaseElements import Resource, Node, SoftItem, Workflow

GA_SPECIE = "GASpecie"
RESOURCE_CONFIG_SPECIE = "ResourceConfigSpecie"

# # TODO: move it to experiments/five_runs/
# # TODO: obsolete, remove it later.
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


def get_max_resource_number(ga_individual):
    current_max = ga_individual[0][1]
    for task in ga_individual:
        if task[1] > current_max:
            current_max = task[1]
    return current_max


def one_to_one_vm_build_solutions(pops, interact_count):

    def is_found_pair(current_tmp_ga_number, res_pop_current_index, pairs):
        if current_tmp_ga_number in pairs:
            ga_res_list = pairs[current_tmp_ga_number]
            if res_pop_current_index in ga_res_list:
                return True

    already_found_pairs = 0
    found_pairs = {}

    ga_pop = []
    ga_name = ''
    res_pop = []
    res_name = ''
    for s, p in pops.items():
        if type(p[0][0]) is Node:
            res_pop = p
            res_name = s.name
        else:
            ga_pop = p
            ga_name = s.name

    no_similar = False;

    for i in range(0,len(ga_pop) - 1) :
        for j in range (0, len(ga_pop[i])):
            if ga_pop[i][j][0] != ga_pop[i+1][j][0] :
                no_similar = True;

    if not no_similar :
        print("====================================================================Similar found");
    not_valid = set()
    while already_found_pairs < interact_count:
        # ga_pop = pops[GA_SPECIE]
        # res_pop = pops[RESOURCE_CONFIG_SPECIE]
        pair_not_found = True

        ga_elem_number = random.randint(0, len(ga_pop))
        while ga_elem_number in not_valid:
            ga_elem_number = random.randint(0, len(ga_pop))

        current_ga_index = 0

        while pair_not_found and current_ga_index < len(ga_pop):
            res_elem_number = random.randint(0, len(res_pop))
            current_tmp_ga_number = (ga_elem_number + current_ga_index) % len(ga_pop)
            ga_individual = ga_pop[current_tmp_ga_number]
            ga_max_res_number = get_max_resource_number(ga_individual)

            for i in range(len(res_pop)):
                res_pop_current_index = (res_elem_number + i) % len(res_pop)
                res_individual = res_pop[res_pop_current_index]
                if len(res_individual) > ga_max_res_number and \
                        not is_found_pair(current_tmp_ga_number, res_pop_current_index, found_pairs):
                    if current_tmp_ga_number not in found_pairs:
                        found_pairs[current_tmp_ga_number] = set()
                    found_pairs[current_tmp_ga_number].add(res_pop_current_index)
                    pair_not_found = False
                    break
            current_ga_index += 1

        if not pair_not_found:
            already_found_pairs += 1
        else:
            not_valid.add(ga_elem_number)
            print('set size is: ' + str(len(not_valid)) + ' added ' + str(ga_elem_number))
            # assert not pair_not_found, "Pair of scheduling and resource organization"
    # elts = [[(s, p) for p in pop] for s, pop in pops.items()]

    # solutions = [DictBasedIndividual({s.name: pop for s, pop in el}) for el in zip(*elts)]
    solutions = [DictBasedIndividual({res_name: res_pop[ls], ga_name: ga_pop[ga_num]}) for
                 ga_num, ls_numbers in found_pairs.items() for ls in ls_numbers]

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
            values[0] += float((sol.fitness - mn) * k + sol.fitness) / len(sol)
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
# pop = ctx[name]
# assert len(pop) == size, "Size of predefined population doesn't match to required size: {0} vs {1}"\
# .format(len(pop), size)
# return pop


def _check_precedence(workflow, seq):
    for i in range(len(seq)):
        task = workflow.byId(seq[i])
        pids = [p.id for p in task.parents]
        for j in range(i + 1, len(seq)):
            if seq[j] in pids:
                return False
    return True


def ga2resources_build_schedule(workflow, estimator, resource_manager, solution):
    gs = solution[GA_SPECIE]
    rs = solution[RESOURCE_CONFIG_SPECIE]
    if (get_max_resource_number(gs) > len(rs) - 1):
        print('found')
    check_consistency(workflow, gs)

    ms = {t: rs[n] for t, n in gs}
    schedule_mapping = {n: [] for n in set(ms.values())}
    task_to_node = {}
    for t in gs:
        node = ms[t[0]]
        task = workflow.byId(t[0])
        (start_time, end_time) = place_task_to_schedule(workflow,
                                                        estimator,
                                                        schedule_mapping,
                                                        task_to_node,
                                                        ms, task, node, 0)

        task_to_node[task.id] = (node, start_time, end_time)
    schedule = Schedule(schedule_mapping)
    return schedule


def mapping2order_build_schedule(workflow, estimator, resource_manager, solution):
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
    #schedule = build_schedule(env.wf, env.estimator, env.rm, solution)
    schedule = mapping2order_build_schedule(env.wf, env.estimator, env.rm, solution)
    result = Utility.makespan(schedule)
    # result = ExecutorRunner.extract_result(schedule, True, workflow)
    return -result


def fitness_ga_and_vm(ctx, solution):
    env = ctx['env']
    schedule = ga2resources_build_schedule(env.wf, env.estimator, env.rm, solution)
    result = Utility.makespan(schedule)
    # result = ExecutorRunner.extract_result(schedule, True, workflow)
    return -result


def overhead_fitness_mapping_and_ordering(ctx,
                                          solution):
    env = ctx['env']
    # schedule = build_schedule(env.wf, env.estimator, env.rm, solution)

    task_to_node = {t: n for t, n in solution[MAPPING_SPECIE]}
    unique_tasks = env.wf.get_all_unique_tasks()
    transfer_overheads = 0
    for task in unique_tasks:
        tnode = task_to_node[task.id]
        ## TODO: this is hack. See parametres
        transfer_overheads += sum(
            [env.estimator.estimate_transfer_time(env.rm.byName(tnode), env.rm.byName(task_to_node[p.id]), task, p) for
             p in task.parents if p.id != env.wf.head_task.id])

    compute_overheads = sum(
        [env.estimator.estimate_runtime(task, env.rm.byName(task_to_node[task.id])) for task in unique_tasks])

    # result = Utility.makespan(schedule)
    # result = ExecutorRunner.extract_result(schedule, True, workflow)
    return -(transfer_overheads + compute_overheads)



## TODO: very simple version, As a ResourceConfig specie It will have to be extended to apply deeper analysis of situations
def fitness_ordering_resourceconf(workflow,
                                  estimator,
                                  solution):
    os = solution[ORDERING_SPECIE]
    rcs = solution[RESOURCE_CONFIG_SPECIE]
    # # TODO: refactor this
    flops_set = [conf.flops for conf in rcs if conf is not None]
    resources = ResourceGenerator.r(flops_set)
    resource_manager = ExperimentResourceManager(resources)
    heft = DynamicHeft(workflow, resource_manager, estimator, os)
    schedule = heft.run({n: [] for n in resource_manager.get_nodes()})
    result = Utility.makespan(schedule)
    return 1 / result

# #====================================
# #Mapping specie
# #====================================
"""
chromosome = [(task_id, node_name), ...]
"""


def mapping_default_initialize(ctx, size):
    env = ctx['env']
    nodes = list(env.rm.get_nodes())
    tasks = sorted(list(env.wf.get_all_unique_tasks()), key=lambda x: x.id)
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
        if random.random() < 1 / len(mutant):
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
        if random.random() < k / len(mutant):
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
        if random.random() < k / len(mutant):
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
        if random.random() < k / len(mutant):
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
        ptransfer_time = [ttime(node, env.rm.byName(task_to_node[p.id]), task, p) for p in task.parents if
                          p != env.wf.head_task]
        ctransfer_time = [ttime(node, env.rm.byName(task_to_node[p.id]), task, p) for p in task.children]
        computation_time = env.estimator.estimate_runtime(task, node)
        return (ptransfer_time, ctransfer_time, computation_time)

    overheads = {t: estimate_overheads(t, n) for t, n in task_to_node.items()}
    sorted_overheads = sorted(overheads.items(), key=lambda x: x[1][0] + x[1][1])

    ## choose overhead for improving
    # try to improve max transfer overhead
    # t, oheads = sorted_overheads[0]
    for i in range(50):
        t, oheads = sorted_overheads[random.randint(0, int(len(sorted_overheads) / 2))]

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
## RC specie
##==================================


def vm_resource_default_initialize(ctx, size):
    env = ctx['env']
    fc = env.rm.farm_capacity
    mrc = env.rm.max_resource_capacity
    max_sweep_size = env.wf.get_max_sweep() / 2
    default_inited_pop = []
    random_values = ""
    result = []
    for i in range(size):
        res = Resource('r1')
        current_cap = 0
        generated_vms = []
        n = -1

        while current_cap < fc - mrc and n < max_sweep_size:
            n += 1
            tmp_capacity = random.randint(1, mrc)
            random_values += str(tmp_capacity) + " "
            #tmp_node = Node('n' + str(n), res, SoftItem.ANY_SOFT)
            tmp_node = Node(n, res, [SoftItem.ANY_SOFT])
            tmp_node.flops = tmp_capacity
            generated_vms.append(tmp_node)
            #res.nodes.add(tmp_node)
            current_cap += tmp_capacity

        if current_cap < fc and n < max_sweep_size:
            n += 1
            #tmp_node = Node('n' + str(n), res, SoftItem.ANY_SOFT)
            tmp_node = Node(n, res, [SoftItem.ANY_SOFT])
            tmp_node.flops = fc - current_cap
            generated_vms.append(tmp_node)
            #res.nodes.add(tmp_node)

        default_inited_pop.append(generated_vms)

        for s in default_inited_pop:
            all_flops = sum(tmp.flops for tmp in s)
            if (all_flops > fc):
                print ("=============wrong initialization " + all_flops)
            for tmp in s:
                if tmp.flops < 1: print('=============wrong initialization ' + tmp.flops)
        result = [ListBasedIndividual(s) for s in default_inited_pop]

    print('vm initialization complited : ' + random_values)
    return result


def resource_conf_crossover(ctx, child1, child2):
    env = ctx['env']
    fc = env.rm.farm_capacity

    filled_power = sum(s.flops for s in child2)
    if filled_power > fc:
        print('================= wrong value of flops before crossover child2 ' + str(filled_power))
        return

    filled_power = sum(s.flops for s in child1)
    if filled_power > fc:
        print('================= wrong value of flops before crossover child1 ' + str(filled_power))
        return


    def get_child_from_pair(p1, p2, k):
        filled_power = sum(s.flops for s in p1[0:k])

        i = k
        while filled_power < fc and i < len(p2):
            filled_power += p2[i].flops
            i += 1

        if filled_power > fc:
            i += -2

        if i >= len(p2) or filled_power == fc:
            i += -1

        new_part = [deepcopy(p2[p_tmp]) for p_tmp in range(k, i + 1)]
        old_part = [deepcopy(p1[p_tmp]) for p_tmp in range(0, k)]

        for j in range(0, len(new_part)):
            new_part[j].name = k + j

        nch = old_part + new_part
        ch_sum = sum(s.flops for s in nch)
        if (ch_sum - fc) > 0:
            print('================sum after crossover ' + str(ch_sum) + ' ' + str(fc))
        return nch

    if len(child1) > 2:
        k = random.randint(0, len(child1) - 2)
        first = get_child_from_pair(child1, child2, k)
        if len(child2) > 2:
            k = random.randint(0, len(child2) - 2)
            second = get_child_from_pair(child2, child1, k)
        else:
            k = random.randint(0, len(child1) - 2)
            second = get_child_from_pair(child1, child2, k)
        child1.clear()
        child1.extend(first)
        child2.clear()
        filled_power = sum(s.flops for s in child2)
        child2.extend(second)

    filled_power = sum(s.flops for s in child2)
    if filled_power > fc:
        print('================= wrong value of flops after crossover child2 ' + str(filled_power))

    filled_power = sum(s.flops for s in child1)
    if filled_power > fc:
        print('================= wrong value of flops after crossover child1 ' + str(filled_power))

    pass


def resource_config_mutate(ctx, mutant):
    env = ctx['env']
    fc = env.rm.farm_capacity
    rc = env.rm.max_resource_capacity

    filled_power = sum(s.flops for s in mutant)
    if filled_power > fc :
        print("================= wrong chromosome at the start of mutate phase " + str(filled_power))
        #return
    def try_to_decrease_resources(mutant, k1):

        str_po_print = 'd ' + str(len(mutant)) + ' '

        flops_to_share = mutant.pop(k1).flops

        if flops_to_share < 0:
            print('================= wrong value of flops to share ' + str(flops_to_share))

        for i in range(len(mutant)):
            k_tmp = random.randint(0, len(mutant) - 1)
            value_to_add = min(rc - mutant[k_tmp].flops, flops_to_share)
            mutant[k_tmp].flops += value_to_add
            flops_to_share -= value_to_add
            if flops_to_share <= 0:
                if flops_to_share < 0:
                    print('================= wrong value of flops to share ' + str(flops_to_share))
                break

        return str_po_print + str(len(mutant))

    def try_to_increase_resources(mutant, k1, k2):
        str_po_print = 'i ' + str(len(mutant)) + ' '
        tmp_node = Node(k1, mutant[0].resource, [SoftItem.ANY_SOFT])
        if fc - filled_power < 1:
            print('wrong operation type')
        tmp_node.flops = min(fc - filled_power, rc)

        mutant.insert(k1, tmp_node)

        for i in range(k1 + 1, len(mutant)):
            mutant[i].name = i
        k_tmp = random.randint(0, len(mutant) - 1)

        if tmp_node.flops < rc:

            while k_tmp == k1 or mutant[k_tmp].flops <= 1:
                k_tmp = random.randint(0, len(mutant) - 1)
            value_to_add = random.randint(1, min(rc - tmp_node.flops, mutant[k_tmp].flops - 1))
            tmp_node.flops += value_to_add
            mutant[k_tmp].flops -= value_to_add
            if mutant[k_tmp].flops < 1 or tmp_node.flops < 1:
                print(
                    '=================increase resource wrong logic ' + str(mutant[k_tmp].flops) + ' valuetoadd ' + str(
                        value_to_add) + ' tmpNode ' + str(tmp_node.flops))
        return str_po_print + str(len(mutant))

    def try_to_change_resource_options(mutant, k1, k2):
        left_res_cap = fc - filled_power
        if left_res_cap < 0:
            print('===============negative flops amount left to add :' + str(left_res_cap))
        mutant[k1].flops = min(rc, mutant[k1].flops + left_res_cap)
        val_to_add = rc - mutant[k1].flops
        if val_to_add > 0:
            if mutant[k2].flops - 1 < 0:
                print('=================negative flops amount in mutant[k2] before operation: ' + str(
                    mutant[k2].flops - 1))
            flops_to_change = random.randint(0, min(mutant[k2].flops - 1, val_to_add))
            mutant[k1].flops += flops_to_change
            mutant[k2].flops -= flops_to_change
            if mutant[k2].flops - 1 < 0:
                print('================negative flops amount after : ' + str(mutant[k2].flops - 1))

    k1, k2 = 0, 0

    if filled_power > fc:
            print('================= wrong value of flops before all' + str(filled_power))

    while k1 == k2 and len(mutant) > 1:
        k1 = random.randint(0, len(mutant) - 1)
        k2 = random.randint(0, len(mutant) - 1)

    option = random.random()

    if option < 1 / 3 and k1 != k2:
        try_to_change_resource_options(mutant, k1, k2)
        filled_power = sum(s.flops for s in mutant)
        if filled_power > fc:
            print('================= wrong value of flops after resource changes' + str(filled_power))
    elif option < 2 / 3 and k1 != k2:
        try_to_decrease_resources(mutant, k1)
        filled_power = sum(s.flops for s in mutant)
        if filled_power > fc:
            print('================= wrong value of flops after resource decrease' + str(filled_power))
    elif (k1 != k2) and option > 2 / 3 and filled_power < fc:
        try_to_increase_resources(mutant, k1, k2)
        filled_power = sum(s.flops for s in mutant)
        if filled_power > fc:
            print('================= wrong value of flops after resource increase' + str(filled_power))

    filled_power = sum(s.flops for s in mutant)
    if filled_power > fc:
            print('================= wrong value of flops after all' + str(filled_power))


##===================================
## GA specie
##==================================

def ga_default_initialize(ctx, size):
    env = ctx['env']
    tasks = ({task.id: task} for task in env.wf.get_all_unique_tasks())

    #result = [ListBasedIndividual((t.id, 'n' + str(random.randint(0, res_amount))) for t in tasks)
    #          for i in range(size)]
    result = []
    chromo = [task for task in env.wf.get_all_unique_tasks()]
    found = True
    while found:
        found = False
        for i in range(len(chromo)):
            prnts = chromo[i].parents
            ind_to_change = i
            for j in range(i+1, len(chromo)):
                if chromo[j] in prnts:
                    ind_to_change = j
            if i != ind_to_change:
                val = chromo.pop(i)
                chromo.insert(ind_to_change, val)
                found = True
                break
    for i in range(size):
        res_amount = vm_random_count_generate(ctx)
        ls = ListBasedIndividual((t.id, random.randint(0, res_amount)) for t in chromo)
        result.append(ls)
    return result


def vm_random_count_generate(ctx):
    env = ctx['env']
    fc = env.rm.farm_capacity
    mrc = env.rm.max_resource_capacity
    max_sweep_size = env.wf.get_max_sweep() / 2
    currentCap = 0
    n = -1
    while currentCap < fc - mrc and n < max_sweep_size:
        n += 1
        tmp_capacity = random.randint(0, mrc)
        currentCap += tmp_capacity
    if currentCap < fc and n < max_sweep_size:
        n += 1
    return random.randint(0, n)


def ga_mutate(ctx, mutant):
    k = len(mutant) / 25
    env = ctx['env']
    wf = env.wf
    for i in range(len(mutant)):
        if random.random() < k / len(mutant):
            found_inconsistency = True
            while found_inconsistency:
                k1 = random.randint(0, len(mutant) - 1)
                k2 = random.randint(0, len(mutant) - 1)
                mx, mn = max(k1, k2), min(k1, k2)

                anc_mn = wf.ancestors(mutant[mn][0])

                found_inconsistency = False

                for i in range(mn, mx):
                    if mutant[i][0] in anc_mn:
                        found_inconsistency = True
                        break

                if not found_inconsistency:
                    prnts = [task.id for task in wf.byId(mutant[mx][0]).parents]
                    for i in range(mn, mx):
                        if mutant[i][0] in prnts:
                            found_inconsistency = True
                            break

            mutant[k1], mutant[k2] = mutant[k2], mutant[k1]

            check_consistency(ctx, mutant)

            cell = random.randint(0, len(mutant) - 1)
            res = random.randint(0, max(c[1] for c in mutant))

            mutant[cell] = (mutant[cell][0], res)
    if random.random() < k / (2 * len(mutant)):
        num = get_max_resource_number(mutant)
        for i in range(len(mutant)):
            if random.random() < k / len(mutant):
                k1 = random.randint(0, len(mutant) - 1)
                res_number = random.randint(0, num + 1)
                mutant[k1] = (mutant[k1][0], res_number)
    elif random.random() < k / (2 * len(mutant)):
        num = get_max_resource_number(mutant)
        if (num > 0):
            for i in range(len(mutant)):
                if mutant[i][1] == num:
                    res_number = random.randint(0, num - 1)
                    mutant[i] = (mutant[i][0], res_number)

    check_consistency(ctx, mutant)
    return mutant


def ga_crossover(ctx, child1, child2):
    env = ctx['env']
    wf = env.wf
    i1 = random.randint(0, len(child1))
    i2 = random.randint(0, len(child1))

    index1 = min(i1, i2)
    index2 = max(i1, i2)

    def make_offspring(ch1, ch2):
        global fe1
        f1 = ch1[0:index1]
        f2 = ch1[index1:index2]
        f3 = ch1[index2:]

        s1 = ch2[0:index1]
        s2 = ch2[index1:index2]
        s3 = ch2[index2:]

        diff_s1_f2 = [(se1, se2) for (se1, se2) in s1 if se1 not in [fe1 for (fe1, fe2) in f2]]
        diff_s3_f2 = [(se1, se2) for (se1, se2) in s3 if se1 not in [fe1 for (fe1, fe2) in f2]]
        diff_s2_f2 = [(s2[i], i) for i in range(len(s2)) if s2[i][0] not in [fe1 for (fe1, fe2) in f2]]

        merged_f2_s2 = ListBasedIndividual(s for s in f2)

        def insert_cell(cell, cromo_part):
            t = wf.byId(cell[0][0])
            pos = cell[1]
            i = pos
            while i < len(cromo_part):
                if wf.byId(cromo_part[i][0]) in t.parents:
                    pos = i + 1
                i += 1
            cromo_part.insert(pos, cell[0])

        j = 0
        while len(diff_s2_f2) > j:
            insert_cell(diff_s2_f2[j], merged_f2_s2)
            j += 1
        return list(diff_s1_f2 + merged_f2_s2 + diff_s3_f2)

    chrm1 = make_offspring(child1, child2)
    chrm2 = make_offspring(child2, child1)

    check_consistency(ctx, chrm1)
    check_consistency(ctx, chrm2)

    return chrm1, chrm2


def check_consistency(ctx, chromosome):
    if type(ctx) is Workflow:
        wf = ctx
    else:
        env = ctx['env']
        wf = env.wf

    for i in range(len(chromosome)):
        prnts = [parent.id for parent in wf.byId(chromosome[i][0]).parents]
        for k in range(i + 1, len(chromosome)):
            if chromosome[k][0] in prnts:
                assert True, "wrong" + str(chromosome[i][0])


##===================================
## Ordering specie
##==================================

def ordering_heft_based_initialize(ctx, size, heft_ordering, count):
    result = [deepcopy(heft_ordering) for i in range(count)]
    result = result + ordering_default_initialize(ctx, size - count)
    return result


def ordering_default_initialize(ctx, size):
    env = ctx['env']
    sorted_tasks = HeftHelper.heft_rank(env.wf, env.rm, env.estimator)

    assert check_precedence(env.wf, sorted_tasks), "Check precedence failed"

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


def resource_default_crossover(ctx, child1, child2):
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






