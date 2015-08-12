from copy import deepcopy, copy
import random

from deap import tools
import numpy
from numpy.lib.function_base import append, insert
from heft.algs.common.NewSchedulerBuilder import place_task_to_schedule, NewScheduleBuilder
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE, check_precedence
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import Utility
from heft.core.environment.ResourceGenerator import ResourceGenerator
from heft.core.environment.BaseElements import Resource, Node, SoftItem, Workflow
from heft.algs.common.individuals import DictBasedIndividual, ListBasedIndividual

GA_SPECIE = "GASpecie"
RESOURCE_CONFIG_SPECIE = "ResourceConfigSpecie"

def get_max_resource_number(ga_individual):
    # this function returns dict of max used node for each blade
    max_set = dict()
    for task in ga_individual:
        if task[1] not in max_set.keys():
            max_set[task[1]] = task[2]
            continue
        if max_set[task[1]] < task[2]:
            max_set[task[1]] = task[2]
    return max_set

def res_length(rc):
    return {idx: len(rc[idx]) for idx in range(len(rc))}

def resources_length(rc_pop):
    # this function returns dict of node count for each blade
    res_lengths = [res_length(rc) for rc in rc_pop]
    cur_len = dict()
    for res_len in res_lengths:
        for item in res_len.items():
            if item[0] not in cur_len.keys():
                cur_len[item[0]] = item[1]
            else:
                cur_len[item[0]] = max(item[1], cur_len[item[0]])
    return cur_len

def individual_lengths_compare(res_individual, max_nodes):
    for idx in range(len(res_individual)):
        if len(res_individual[idx]) <= max_nodes[idx]:
            return False
    return True

def get_res_by_name(res_list, name):
    for res in res_list:
        if res.name == name:
            return res
    return None

def get_node_by_name(node_list, name):
    for node in node_list:
        if node.name == name:
            return node
    return None

class one_to_one_vm_build_solutions:
    """
    pops should contain two populations
    "GASpecie" and "ResourceConfigSpecie"
    each individual of population "GASpecie" has the following structure
    [(task_id, res_idx, node_idx), ...]

    each individual of "ResourceConfigSpecie":
    [[r1n1, r1n2, r1n3], [r2n1, r2n2..] ...], where each Resource object contains generated nodes configuration
    """
    def __call__(self, pops, interact_count, additional_resources):
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
            if s.name is RESOURCE_CONFIG_SPECIE:
                res_pop = p
                res_name = s.name
            else:
                ga_pop = p
                ga_name = s.name

        not_valid = set()
        while already_found_pairs < interact_count:
            pair_not_found = True

            ga_elem_number = random.randint(0, len(ga_pop))
            choose_counter = 0
            while ga_elem_number in not_valid:
                if choose_counter > 100 and choose_counter < 150:
                    print("ga_elem_number in not_valid = ")
                    print(ga_pop(ga_elem_number))
                choose_counter += 1
                ga_elem_number = random.randint(0, len(ga_pop))

            current_ga_index = 0
            while pair_not_found and current_ga_index < len(ga_pop):
                res_elem_number = random.randint(0, len(res_pop))
                current_tmp_ga_number = (ga_elem_number + current_ga_index) % len(ga_pop)
                ga_individual = ga_pop[current_tmp_ga_number]
                #TODO rename this
                ga_max_res_number = get_max_resource_number(ga_individual)
                for item in ga_max_res_number.items():
                    ga_max_res_number[item[0]] -= additional_resources[item[0]]

                res_individual = None

                for i in range(len(res_pop)):
                    res_pop_current_index = (res_elem_number + i) % len(res_pop)
                    res_individual = res_pop[res_pop_current_index]
                    #print("GA_ind = " + str(ga_individual))
                    #print("RES_ind = " + str(res_individual))
                    # TODO this is weak place now and need to rename functions in condition
                    if individual_lengths_compare(res_individual, ga_max_res_number) and \
                            not is_found_pair(current_tmp_ga_number, res_pop_current_index, found_pairs):
                        if current_tmp_ga_number not in found_pairs:
                            found_pairs[current_tmp_ga_number] = set()
                        found_pairs[current_tmp_ga_number].add(res_pop_current_index)
                        pair_not_found = False
                        break

                # TODO this is crutch, refactoring later(
                if pair_not_found:
                    print("PZDC")
                    max_nodes = res_length(res_individual)
                    for item in max_nodes.items():
                        max_nodes[item[0]] += additional_resources[item[0]]
                    for (elem, idx) in zip(ga_individual, range(len(ga_individual))):

                        # while len(res.nodes) == 0:
                        #     res_individual = res_pop[random.randint(0, len(res_pop) - 1)]
                        #     res = get_res_by_name(res_individual, elem[1])
                        if elem[2] >= max_nodes[elem[1]]:
                            if max_nodes[elem[1]] <= 1:
                                pass
                            ga_individual[idx] = (elem[0], elem[1], random.randint(0, max_nodes[elem[1]] - 1))

                current_ga_index += 1

            if not pair_not_found:
                already_found_pairs += 1
            else:
                not_valid.add(ga_elem_number)
                #print('set size is: ' + str(len(not_valid)) + ' added ' + str(ga_elem_number))
                # assert not pair_not_found, "Pair of scheduling and resource organization"
        # elts = [[(s, p) for p in pop] for s, pop in pops.items()]

        # solutions = [DictBasedIndividual({s.name: pop for s, pop in el}) for el in zip(*elts)]
        solutions = [DictBasedIndividual({res_name: res_pop[ls], ga_name: ga_pop[ga_num]}) for
                     ga_num, ls_numbers in found_pairs.items() for ls in ls_numbers]

        return solutions

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


def chrom_converter(gs, task_map, node_map):
    """
    Convert [task:node] with [nodes] to {node:[tasks]}
    """
    chrom = {}
    for node in node_map.keys():
        chrom[node] = []
    for (task, res, node) in gs:
        chrom[node].append(task)
    return chrom

def live_fixed_nodes(fixed_schedule):
    """
    return nodes with executing tasks in fixed_schedule
    """
    res = []
    for item in fixed_schedule.items():
        if len(item[1]) > 0 and item[1][-1].state == 'executing':
            res.append(item[0])
    return res

def dead_fixed_nodes(fixed_schedule):
    """
    return dead nodes in fixed_schedule
    """
    res = []
    for item in fixed_schedule.items():
        if item[0].state == Node.Down or (len(item[1]) > 0 and item[1][-1].state == 'failed'):
            res.append(item[0])
    return res

def assignment_to_resources(nodes, rm):
    result = dict()
    for idx in range(len(rm.resources)):
        result[idx] = [node for node in nodes if node.resource == rm.resources[idx]]
    return result


def rm_adapt(rm, fixed, rc):
    """
    Adapt resource manager to fixed schedule
    """
    live_nodes = live_fixed_nodes(fixed)
    dead_nodes = dead_fixed_nodes(fixed)

    rc_copy = deepcopy(rc)

    for node in fixed.keys():
        node_res = node.resource
        res_idx = rm.resources.index(node_res)
        if node not in live_nodes and node not in dead_nodes:
            node_copy = deepcopy(node)
            if node.flops not in rc_copy[res_idx]:
                node_copy.state = Node.Down
            else:
                rc_copy[res_idx].remove(node.flops)
            rm.resources[res_idx].nodes.append(node_copy)

        if node in dead_nodes:
            node_copy = deepcopy(node)
            node_copy.state = Node.Down
            rm.resources[res_idx].nodes.append(node_copy)
        if node in live_nodes:
            node_copy = deepcopy(node)
            rm.resources[res_idx].nodes.append(node_copy)
    for res_idx in range(len(rc_copy)):
        n = 0
        used_names = [node.name for node in rm.resources[res_idx].nodes]
        for node_idx in range(len(rc_copy[res_idx])):
            new_name = "res_" + str(res_idx) + "_node_" + str(n)
            while new_name in used_names:
                n += 1
                new_name = "res_" + str(res_idx) + "_node_" + str(n)
            new_node = Node(new_name, rm.resources[res_idx], SoftItem.ANY_SOFT, rc_copy[res_idx][node_idx])
            used_names.append(new_name)
            rm.resources[res_idx].nodes.append(new_node)
    for res in rm.resources:
        rm.resources_map[res.name] = res



def ga2resources_build_schedule(workflow, estimator, resource_manager, solution, ctx):
    """
    return: Schedule
    mapiing = {
        Node: [ScheduleItem(), ...]
        ...
    }
    """
    gs = solution[GA_SPECIE]
    rs = solution[RESOURCE_CONFIG_SPECIE]



    check_consistency(workflow, gs)

    print("_____")
    print("rs = " + str(rs))

    rm = deepcopy(resource_manager)
    for res_idx in range(len(rm.resources)):
        res = rm.resources[res_idx]
        res.nodes = []
        # for node_idx in range(len(rs[res_idx])):
        #     res.nodes.append(Node("res_" + str(res_idx) + "_node_" + str(node_idx), res, SoftItem.ANY_SOFT, rs[res_idx][node_idx]))

    rm_adapt(rm, ctx['fixed_schedule'].mapping, rs)

    live_rm_nodes = []
    for res_idx in range(len(rm.resources)):
        cur_res = rm.resources[res_idx]
        live_res_nodes = [node for node in cur_res.nodes if node.state != Node.Down]
        live_res_nodes.sort(key=lambda x: x.flops, reverse=True)
        live_rm_nodes.append(live_res_nodes)

    gs_adapt = []
    for map_item in gs:
        res = rm.resources[map_item[1]]
        node = live_rm_nodes[map_item[1]][map_item[2]]
        gs_adapt.append((map_item[0], res.name, node.name))
        # TODO this is hack
        if node is None:
            assert("build hack")
            # print("build hack")
            # node = [res_node for res_node in res.nodes][random.randint(0, len(res.nodes) - 1)]
            # bad_idx = gs.index(map_item)
            # map_item = (map_item[0], map_item[1], node.name)
            # gs[bad_idx] = map_item

    task_map = {}
    node_map = {}

    for task in workflow.get_all_unique_tasks():
        task_map[task.id] = task
    for res in rm.resources:
        for node in res.nodes:
            node_map[node.name] = node

    # for node in ctx['fixed_schedule'].mapping.keys():
    #     if node.name not in node_map.keys():
    #         node_map[node.name] = node

    chrom = chrom_converter(gs_adapt, task_map, node_map)
    # try:
    builder = NewScheduleBuilder(workflow, rm, estimator, task_map, node_map, ctx['fixed_schedule'])

    schedule = builder(chrom, ctx['current_time'])
    # except Exception:
    #     pass
    return schedule

def fitness_ga_and_vm(ctx, solution):
    env = ctx['env']
    schedule = ga2resources_build_schedule(env.wf, env.estimator, env.rm, solution, ctx)
    result = Utility.makespan(schedule)
    #result += nodes_overhead_estimate(env.rm.resources, solution["ResourceConfigSpecie"])
    return -result

# Estimate overheads of startup or shutdown nodes
# TODO move shutdown and start costs in estimator
# def nodes_overhead_estimate(rm, sol):
#     # Functions for estimate shutdowns and starts
#     def shutdown_node(node):
#         return 0
#     def start_node(node):
#         return 0
#
#     overhead = 0
#     for res_idx in range(len(rm)):
#         rm_res = rm[res_idx].nodes
#         sol_res = sol[res_idx].nodes
#         for node in sol_res:
#             if node not in rm_res:
#                 overhead += start_node(node)
#         for node in rm_res:
#             if node not in sol_res:
#                 overhead += shutdown_node(node)
#
#     return overhead


##===================================
## RC specie
##==================================

def vm_resource_default_initialize(ctx, size):
    """
    result representation [ListBasedIndividuals([[b1n1, b1n2, ...], [b2n1, b2n2, ...]])]
    """

    env = ctx['env']
    result = []

    for i in range(size):

        max_sweep_size = env.wf.get_max_sweep()
        default_inited_pop = []
        resources = env.rm.get_live_resources()
        for res in resources:
            current_cap = 0
            generated_vms = []
            n = -1
            fc = res.farm_capacity
            mrc = res.max_resource_capacity
            while current_cap < fc - mrc and n < max_sweep_size:
                n += 1
                tmp_capacity = random.randint(1, mrc)
                generated_vms.append(tmp_capacity)
                current_cap += tmp_capacity
            if current_cap < fc and n < max_sweep_size:
                n += 1
                cap = fc - current_cap
                generated_vms.append(cap)

            generated_vms.sort(reverse=True)
            default_inited_pop.append(generated_vms)

        result.append(default_inited_pop)
    result_list = [ListBasedIndividual(s) for s in result]
    return result_list

def resource_conf_crossover(ctx, parent1, parent2):

    def get_child_from_pair(p1, p2, k):
        filled_power = sum(p1[0:k])

        i = k
        while filled_power < fc and i < len(p2):
            filled_power += p2[i]
            i += 1

        if filled_power > fc:
            i += -2

        if i >= len(p2) or filled_power == fc:
            i += -1

        new_part = [deepcopy(p2[p_tmp]) for p_tmp in range(k, i + 1)]
        old_part = [deepcopy(p1[p_tmp]) for p_tmp in range(0, k)]

        nch = old_part + new_part
        ch_sum = sum(nch)
        if (ch_sum - fc) > 0:
            print('================sum after crossover ' + str(ch_sum) + ' ' + str(fc))
        return nch

    env = ctx['env']

    child1 = deepcopy(parent1)
    child2 = deepcopy(parent2)
    for bl_idx in range(len(child1)):
        blade1 = child1[bl_idx]
        blade2 = child2[bl_idx]
        fc = env.rm.resources[bl_idx].farm_capacity

        filled_power = sum(blade2)
        if filled_power > fc:
            print('================= wrong value of flops before crossover child2 ' + str(filled_power))
            return

        filled_power = sum(blade1)
        if filled_power > fc:
            print('================= wrong value of flops before crossover child1 ' + str(filled_power))
            return

        first = None
        second = None
        if len(blade1) > 1:
            k = random.randint(0, len(blade1) - 1)
            first = get_child_from_pair(blade1, blade2, k)

            if len(blade2) > 1:
                k = random.randint(0, len(blade2) - 1)
                second = get_child_from_pair(blade2, blade1, k)
            else:
                k = random.randint(0, len(blade1) - 1)
                second = get_child_from_pair(blade1, blade2, k)

        if first is None:
            first = deepcopy(blade1)
        if second is None:
            second = deepcopy(blade2)

        filled_power = sum(blade2)
        if filled_power > fc:
            print('================= wrong value of flops after crossover child2 ' + str(filled_power))

        filled_power = sum(blade1)
        if filled_power > fc:
            print('================= wrong value of flops after crossover child1 ' + str(filled_power))
        first.sort(reverse=True)
        second.sort(reverse=True)
        child1[bl_idx] = first
        child2[bl_idx] = second
    return child1, child2


def resource_config_mutate(ctx, mutant):

    def try_to_decrease_resources(mutant, k1):

        str_po_print = 'd ' + str(len(mutant)) + ' '

        flops_to_share = mutant.pop(k1)

        if flops_to_share < 0:
            print('================= wrong value of flops to share ' + str(flops_to_share))

        for i in range(len(mutant)):
            k_tmp = random.randint(0, len(mutant) - 1)
            value_to_add = min(rc - mutant[k_tmp], flops_to_share)
            mutant[k_tmp] += value_to_add
            flops_to_share -= value_to_add
            if flops_to_share <= 0:
                if flops_to_share < 0:
                    print('================= wrong value of flops to share ' + str(flops_to_share))
                break

        return str_po_print + str(len(mutant))

    def try_to_increase_resources(mutant, k1):
        str_po_print = 'i ' + str(len(mutant)) + ' '
        if fc - filled_power < 1:
            print('wrong operation type')
        tmp_flops = min(fc - filled_power, rc)
        mutant.append(tmp_flops)

        return str_po_print + str(len(mutant))

    def try_to_change_resource_options(mutant, k1, k2):
        left_res_cap = fc - filled_power
        if left_res_cap < 0:
            print('===============negative flops amount left to add :' + str(left_res_cap))
        mutant[k1] = min(rc, mutant[k1] + left_res_cap)
        val_to_add = rc - mutant[k1]
        if val_to_add > 0:
            if mutant[k2] - 1 < 0:
                print('=================negative flops amount in mutant[k2] before operation: ' + str(
                    mutant[k2].flops - 1))
            flops_to_change = random.randint(0, min(mutant[k2] - 1, val_to_add))
            mutant[k1] += flops_to_change
            mutant[k2] -= flops_to_change
            if mutant[k2] - 1 < 0:
                print('================negative flops amount after : ' + str(mutant[k2].flops - 1))

    env = ctx['env']

    for res, idx in zip(mutant, range(len(mutant))):
        blade = [node for node in res]
        fc = env.rm.resources[idx].farm_capacity
        rc = env.rm.resources[idx].max_resource_capacity

        filled_power = sum(blade)
        if filled_power > fc:
            print("================= wrong chromosome at the start of mutate phase " + str(filled_power))

        k1, k2 = 0, 0

        while len(blade) > 1 and k1 == k2:
            k1 = random.randint(0, len(blade) - 1)
            k2 = random.randint(0, len(blade) - 1)

        option = random.random()

        if option < 1 / 3 and k1 != k2:
            try_to_change_resource_options(blade, k1, k2)
        elif option < 2 / 3 and k1 != k2:
            try_to_decrease_resources(blade, k1)
        elif option > 2 / 3 and filled_power < fc:
            try_to_increase_resources(blade, k1)

        filled_power = sum(blade)
        if filled_power > fc:
            print("================= wrong chromosome at the start of mutate phase " + str(filled_power))
        blade.sort(reverse=True)
        mutant[idx] = blade


##===================================
## GA specie
##==================================

def ga_default_initialize(ctx, size, max_nodes):
    env = ctx['env']
    fix_sched = ctx['fixed_schedule']
    fix_tasks = fix_sched.get_unfailed_taks()

    result = []
    chromo = [task for task in env.wf.get_all_unique_tasks()
              if task not in fix_tasks]

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
        temp = []
        max_res = len(max_nodes.keys())
        for t in chromo:
            res = random.randint(0, max_res - 1)
            node = random.randint(0, max_nodes[res] - 1)
            temp.append((t.id, res, node))
        ls = ListBasedIndividual(temp)
        result.append(ls)

    return result

def ga_mutate(ctx, mutant, max_nodes):
    k = len(mutant) / 25
    env = ctx['env']
    wf = env.wf
    for i in range(len(mutant)):
        if random.random() < 2 * k / len(mutant):
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
    if random.random() < k / (2 * len(mutant)):
        max_res = len(max_nodes.keys())
        for i in range(len(mutant)):
            if random.random() < k / len(mutant):
                k1 = random.randint(0, len(mutant) - 1)
                res_number = random.randint(0, max_res - 1)
                node_number = random.randint(0, max_nodes[res_number] - 1)
                mutant[k1] = (mutant[k1][0], res_number, node_number)

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

        diff_s1_f2 = [(se1, se2, se3) for (se1, se2, se3) in s1 if se1 not in [fe1 for (fe1, fe2, fe3) in f2]]
        diff_s3_f2 = [(se1, se2, se3) for (se1, se2, se3) in s3 if se1 not in [fe1 for (fe1, fe2, fe3) in f2]]
        diff_s2_f2 = [(s2[i], i) for i in range(len(s2)) if s2[i][0] not in [fe1 for (fe1, fe2, fe3) in f2]]
        if len(diff_s2_f2) > 0:
            pass

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
    return ListBasedIndividual(chrm1), ListBasedIndividual(chrm2)


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