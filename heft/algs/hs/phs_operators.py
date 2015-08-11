from copy import deepcopy, copy
import random

import numpy
from heft.algs.common.NewSchedulerBuilder import place_task_to_schedule, NewScheduleBuilder
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import Utility
from heft.core.environment.BaseElements import Node, SoftItem, Workflow
from heft.algs.common.individuals import ListBasedIndividual

GA_SPECIE = "GASpecie"
RESOURCE_CONFIG_SPECIE = "ResourceConfigSpecie"

def get_max_resource_number(ga_individual):
    # this function returns dict of max used node for each blade
    max_set = dict()
    max_set[ga_individual[0][1]] = [ga_individual[0][2]]
    for task in ga_individual:
        if task[1] not in max_set.keys() or task[2] not in max_set[task[1]]:
            if task[1] not in max_set.keys():
                max_set[task[1]] = [task[2]]
            else:
                max_set[task[1]].append(task[2])
    return max_set

def individual_lengths_compare(res_individual, used_resources):
    for res in res_individual:
        for node in used_resources[res.name]:
            if node not in [res_node.name for res_node in res.nodes]:
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

def _check_precedence(workflow, seq):
    for i in range(len(seq)):
        task = workflow.byId(seq[i])
        pids = [p.id for p in task.parents]
        for j in range(i + 1, len(seq)):
            if seq[j] in pids:
                return False
    return True

def chrom_converter(gs, task_map, node_map):
    """
    Convert [task:node] with [nodes] to {node:[tasks]}
    """
    chrom = {}
    for node in node_map.keys():
        chrom[node] = []
    for (task, node) in gs:
        chrom["res_0_node_" + str(node)].append(task)
    return chrom

def ga2resources_build_schedule(workflow, estimator, resource_manager, solution, ctx):
    gs = solution[0]
    rs = solution[1]

    rm = deepcopy(resource_manager)
    res = rm.resources[0]

    res.nodes = []
    for idx, cap in enumerate(rs):
        res.nodes.append(Node("res_0_node_" + str(idx), res, SoftItem.ANY_SOFT, cap))
    for idx, map_item in enumerate(gs):
        # Adaptation
        if map_item[1] >= len(res.nodes):
            # print("PRIVET")
            gs[idx] = (map_item[0], random.randint(0, len(res.nodes) - 1))
            # gs[idx] = (map_item[0], 0)

    task_map = {}
    node_map = {}

    for task in workflow.get_all_unique_tasks():
        task_map[task.id] = task
    for node in res.nodes:
        node_map[node.name] = node

    chrom = chrom_converter(gs, task_map, node_map)

    builder = NewScheduleBuilder(workflow, rm, estimator, task_map, node_map, ctx['fixed_schedule'])

    schedule = builder(chrom, 0)
    return schedule

def fitness_ga_and_vm(ctx, solution):
    env = ctx['env']
    schedule = ga2resources_build_schedule(env.wf, env.estimator, env.rm, solution, ctx)
    result = Utility.makespan(schedule)
    return -result

##===================================
## RC specie
##==================================


def vm_resource_default_initialize(ctx, size):
    env = ctx['env']
    result = []

    for i in range(size):
        max_sweep_size = 3 #env.wf.get_max_sweep()
        res = env.rm.resources[0]
        current_cap = 0
        generated_vms = []
        fc = res.farm_capacity
        mrc = res.max_resource_capacity
        n = -1
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
        result.append(ListBasedIndividual(generated_vms))
    return result

def resource_conf_crossover(ctx, parent1, parent2):
    env = ctx['env']
    child = []
    res = env.rm.resources[0]
    fc = res.farm_capacity
    cur_cap = 0
    idx1 = 0
    idx2 = 0
    parent2.reverse()
    while cur_cap < fc:
        if idx1 == idx2 and idx1 < len(parent1):
            node_cap = parent1[idx1]
            if node_cap > fc - cur_cap:
                child.append(fc - cur_cap)
                cur_cap += fc - cur_cap
            else:
                child.append(parent1[idx1])
                cur_cap += node_cap

            idx1 += 1
        elif idx2 < len(parent2):
            node_cap = parent2[idx2]
            if node_cap > fc - cur_cap:
                child.append(fc - cur_cap)
                cur_cap += fc - cur_cap
            else:
                child.append(parent2[idx2])
                cur_cap += node_cap
            idx2 += 1
        else:
            break
    child.sort(reverse=True)
    return ListBasedIndividual(child)


def resource_config_mutate(ctx, mutant):

    def try_to_decrease_resources(mutant, k):
        if len(mutant) > 1:
            remove_val = mutant.pop(k)
            for idx in range(len(mutant)):
                if mutant[idx] < mrc and remove_val > 0:
                    rem = min(remove_val, mrc - mutant[idx])
                    mutant[idx] += rem
                    remove_val -= rem
        #power = sum(mutant)
        #pos_cap = fc - power
        #while pos_cap > 0:
        #    add = min(pos_cap, mrc)
        #    pos_cap -= add
        #    mutant.append(add)
        pass

    def try_to_increase_resources(mutant, k):
        power = sum(mutant)
        pos_cap = fc - power
        if pos_cap > 0:
            mutant.append(min(mrc, pos_cap))
        else:
            if mutant[k] > 2:
                change = random.randint(1, mutant[k] - 1)
                mutant[k] -= change
                mutant.append(change)
        pass

    def try_to_change_resource_options(mutant, k):
        node_cap = mutant[k]
        power = sum(mutant)
        pos_cap = fc - power
        node_up = min(mrc - node_cap, pos_cap - node_cap)
        if random.random() > 0.5 and node_up > 0:
            mutant[k] = node_cap + random.randint(1, node_up)
        elif node_cap > 1:
            mutant[k] = node_cap - random.randint(1, node_cap - 1)
        pass
    env = ctx['env']
    res = env.rm.resources[0]
    fc = res.farm_capacity
    mrc = res.max_resource_capacity
    k = random.randint(0, len(mutant) - 1)
    option = random.random()
    p1 = 0.33
    p2 = 0.66
    # print("Mutate")
    # print("\t" + str(mutant))
    if option < p1:
        # print("change")
        try_to_change_resource_options(mutant, k)
    elif option < p2:
        # print("decrease")
        try_to_decrease_resources(mutant, k)
    else:
        # print("increase")
        try_to_increase_resources(mutant, k)

    mutant.sort(reverse=True)
    # print("\t" + str(mutant))
    pass

##===================================
## GA specie
##==================================

def ga_default_initialize(ctx, size):
    env = ctx['env']

    result = []
    chromo = [task for task in env.wf.get_all_unique_tasks()]
    random.shuffle(chromo)

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
        for t in chromo:
            node_len = len(env.rm.resources[0].nodes)
            temp.append((t.id, random.randint(0, node_len - 1)))
        ls = ListBasedIndividual(temp)
        result.append(ls)

    return result

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

    for i in range(len(mutant)):
        if random.random() < k / len(mutant):
            k1 = random.randint(0, len(mutant) - 1)
            node_len = len(env.rm.resources[0].nodes)
            mutant[k1] = (mutant[k1][0], random.randint(0, node_len - 1))
    pass

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
    return ListBasedIndividual(chrm1)