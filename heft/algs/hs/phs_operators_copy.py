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
    for (task, res, node) in gs:
        chrom[node].append(task)
    return chrom

def ga2resources_build_schedule(workflow, estimator, resource_manager, solution, ctx):
    """
    return: Schedule
    mapiing = {
        Node: [ScheduleItem(), ...]
        ...
    }
    """
    gs = solution[0]
    rs = solution[1]

    check_consistency(workflow, gs)

    rm = deepcopy(resource_manager)
    for res in rs:
        if res.name not in [elem.name for elem in rm.resources]:
            rm.resources.append(res)
        else:
            for node in res.nodes:
                rm_res = get_res_by_name(rm.resources, res.name)
                if node.name not in [elem.name for elem in rm_res.nodes]:
                    rm_res.nodes.append(node)
    if 'cemetery' in ctx.keys():
        for node in ctx['cemetery']:
            temp_node = deepcopy(node)
            #temp_node.state = Node.Unknown
            rm_res = get_res_by_name(rm.resources, node.resource.name)
            rm_res.nodes.append(temp_node)

    for map_item in gs:
        res = get_res_by_name(rs, map_item[1])
        node = get_node_by_name(res.nodes, map_item[2])

        # TODO this is hack
        if node is None:
            print("HACK!!!")
            node = [res_node for res_node in res.nodes][random.randint(0, len(res.nodes) - 1)]
            bad_idx = gs.index(map_item)
            map_item = (map_item[0], map_item[1], node.name)
            gs[bad_idx] = map_item

    task_map = {}
    node_map = {}

    for task in workflow.get_all_unique_tasks():
        task_map[task.id] = task
    for res in resource_manager.resources:
        for node in res.nodes:
            node_map[node.name] = node
    for res in rs:
        for node in res.nodes:
            if node.name not in node_map.keys():
                node_map[node.name] = node
    if 'cemetery' in ctx.keys():
        for node in ctx['cemetery']:
            temp_node = deepcopy(node)
            #temp_node.state = Node.Unknown
            node_map[temp_node.name] = temp_node

    chrom = chrom_converter(gs, task_map, node_map)

    builder = NewScheduleBuilder(workflow, rm, estimator, task_map, node_map, ctx['fixed_schedule'])

    schedule = builder(chrom, 0)
    return schedule

def fitness_ga_and_vm(ctx, solution):
    env = ctx['env']
    schedule = ga2resources_build_schedule(env.wf, env.estimator, env.rm, solution, ctx)
    result = Utility.makespan(schedule)
    return -result

def overhead_fitness_mapping_and_ordering(ctx,
                                          solution):
    env = ctx['env']

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

##===================================
## RC specie
##==================================


def vm_resource_default_initialize(ctx, size):
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
            used_nodes = []
            env_names = [node.name for node in res.nodes]
            while current_cap < fc - mrc and n < max_sweep_size:
                possible_nodes = [node for node in res.nodes if node.name not in used_nodes]
                if random.random() < 0.1 and len(possible_nodes) > 0:
                    tmp_node = deepcopy(possible_nodes[random.randint(0, len(possible_nodes) - 1)])
                    used_nodes.append(tmp_node.name)
                    current_cap += tmp_node.flops
                    generated_vms.append(tmp_node)
                    continue
                n += 1
                node_name = res.name + "_node_" + str(n)
                tmp_capacity = random.randint(1, mrc)
                while node_name in (env_names + used_nodes):
                    n += 1
                    node_name = res.name + "_node_" + str(n)
                tmp_node = Node(node_name, res, [SoftItem.ANY_SOFT], tmp_capacity)
                generated_vms.append(tmp_node)
                used_nodes.append(tmp_node.name)
                current_cap += tmp_capacity
            if current_cap < fc and n < max_sweep_size:
                n += 1
                node_name = res.name + "_node_" + str(n)
                while node_name in (env_names + used_nodes):
                    n += 1
                    node_name = res.name + "_node_" + str(n)
                cap = fc - current_cap
                tmp_node = Node(node_name, res, [SoftItem.ANY_SOFT], cap)
                used_nodes.append(tmp_node.name)
                generated_vms.append(tmp_node)

            for (gen_node, idx) in zip(generated_vms, range(len(generated_vms))):
                possible_nodes = [node for node in res.nodes if gen_node.flops == node.flops and node.name not in used_nodes]
                if len(possible_nodes) > 1:
                    gen_node = deepcopy((possible_nodes[random.randint(0, len(possible_nodes) - 1)]))
                    generated_vms[idx] = gen_node
                    used_nodes.append(gen_node.name)

            new_res = deepcopy(res)
            new_res.nodes = generated_vms
            default_inited_pop.append(new_res)

        result.append(default_inited_pop)
    result_list = [ListBasedIndividual(s) for s in result]
    return result_list

def resource_conf_crossover(ctx, parent1, parent2):

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

        temp_nch = old_part + new_part
        nch = []
        for node in temp_nch:
            if node.name not in [n_node.name for n_node in nch]:
                nch.append(node)
        ch_sum = sum(s.flops for s in nch)
        if (ch_sum - fc) > 0:
            print('================sum after crossover ' + str(ch_sum) + ' ' + str(fc))
        return nch

    env = ctx['env']

    child1 = deepcopy(parent1)
    child2 = deepcopy(parent2)
    for bl_idx in range(len(child1)):
        res = env[1].resources[bl_idx]
        blade1 = [node for node in child1[bl_idx].nodes]
        blade2 = [node for node in child2[bl_idx].nodes]
        fc = env.rm.resources[bl_idx].farm_capacity

        filled_power = sum(s.flops for s in blade2)
        if filled_power > fc:
            print('================= wrong value of flops before crossover child2 ' + str(filled_power))
            return

        filled_power = sum(s.flops for s in blade1)
        if filled_power > fc:
            print('================= wrong value of flops before crossover child1 ' + str(filled_power))
            return

        first = None
        second = None
        if len(blade1) > 2:
            k = random.randint(0, len(blade1) - 2)
            first = get_child_from_pair(blade1, blade2, k)

            if len(blade2) > 2:
                k = random.randint(0, len(blade2) - 2)
                second = get_child_from_pair(blade2, blade1, k)
            else:
                k = random.randint(0, len(blade1) - 2)
                second = get_child_from_pair(blade1, blade2, k)

        if first is not None:
            used_nodes = [node.name for node in first]
            for (gen_node, idx) in zip(first, range(len(first))):
                possible_nodes = [node for node in res.nodes if gen_node.flops == node.flops and node.name not in used_nodes]
                if len(possible_nodes) > 1:
                    new_node = deepcopy(possible_nodes[random.randint(0, len(possible_nodes) - 1)])
                    first[idx] = new_node
                    used_nodes.append(new_node.name)

            blade1.clear()
            blade1.extend(first)
        if second is not None:
            used_nodes = [node.name for node in second]
            for (gen_node, idx) in zip(second, range(len(second))):
                possible_nodes = [node for node in res.nodes if gen_node.flops == node.flops and node.name not in used_nodes]
                if len(possible_nodes) > 1:
                    new_node = deepcopy(possible_nodes[random.randint(0, len(possible_nodes) - 1)])
                    second[idx] = new_node
                    used_nodes.append(new_node.name)

            blade2.clear()
            blade2.extend(second)

        filled_power = sum(s.flops for s in blade2)
        if filled_power > fc:
            print('================= wrong value of flops after crossover child2 ' + str(filled_power))

        filled_power = sum(s.flops for s in blade1)
        if filled_power > fc:
            print('================= wrong value of flops after crossover child1 ' + str(filled_power))

        if first is not None:
            child1[bl_idx].nodes = set(blade1)
        if second is not None:
            child2[bl_idx].nodes = set(blade2)
    return child1, child2


def resource_config_mutate(ctx, mutant):

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
        cur_res = mutant[0].resource.name
        str_po_print = 'i ' + str(len(mutant)) + ' '
        tmp_node = Node(k1, mutant[0].resource, [SoftItem.ANY_SOFT])
        if fc - filled_power < 1:
            print('wrong operation type')
        tmp_node.flops = min(fc - filled_power, rc)
        used_names = [node.name for node in mutant]
        env_res = [res for res in ctx['env'][1].resources if res.name == cur_res][0]
        used_names += [node.name for node in env_res.get_live_nodes()]
        tmp_idx = 0
        tmp_name = cur_res + "_node_" + str(tmp_idx)
        while tmp_name in used_names:
            tmp_idx += 1
            tmp_name = cur_res + "_node_" + str(tmp_idx)
        tmp_node.name = tmp_name
        mutant.insert(k1, tmp_node)

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

    env = ctx['env']

    for res, idx in zip(mutant, range(len(mutant))):
        blade = [node for node in res.nodes]
        fc = env.rm.resources[idx].farm_capacity
        rc = env.rm.resources[idx].max_resource_capacity
        env_nodes = [node for node in env.rm.resources[idx].nodes]
        env_names = [node.name for node in env_nodes]

        filled_power = sum(s.flops for s in blade)
        if filled_power > fc:
            print("================= wrong chromosome at the start of mutate phase " + str(filled_power))

        k1, k2 = 0, 0

        if filled_power > fc:
                print('================= wrong value of flops before all' + str(filled_power))

        counter = 0
        is_static_nodes = False
        while len(blade) > 1 and k1 == k2:
            if counter > len(blade):
                is_static_nodes = True
                break
            counter += 1
            k1 = random.randint(0, len(blade) - 1)
            k2 = random.randint(0, len(blade) - 1)

        option = random.random()

        if option < 1 / 3 and k1 != k2:
            try_to_change_resource_options(blade, k1, k2)
            filled_power = sum(s.flops for s in blade)
            if filled_power > fc:
                print('================= wrong value of flops after resource changes' + str(filled_power))
        elif option < 2 / 3 and k1 != k2:
            try_to_decrease_resources(blade, k1)
            filled_power = sum(s.flops for s in blade)
            if filled_power > fc:
                print('================= wrong value of flops after resource decrease' + str(filled_power))
        elif (k1 != k2) and option > 2 / 3 and filled_power < fc:
            try_to_increase_resources(blade, k1, k2)
            filled_power = sum(s.flops for s in blade)
            if filled_power > fc:
                print('================= wrong value of flops after resource increase' + str(filled_power))

        filled_power = sum(s.flops for s in blade)
        if filled_power > fc:
                print('================= wrong value of flops after all' + str(filled_power))

        # check corectness

        filled_power = sum(s.flops for s in blade)
        if filled_power > fc:
            print("================= wrong chromosome at the start of mutate phase " + str(filled_power))

        used_nodes = [node.name for node in res.nodes]
        temp_nodes = []
        for gen_node in blade:
            possible_nodes = [node for node in env_nodes if gen_node.flops == node.flops and node.name not in used_nodes]
            if len(possible_nodes) > 0:
                gen_node = deepcopy(possible_nodes[random.randint(0, len(possible_nodes) - 1)])
                used_nodes.append(gen_node.name)
            wrong_nodes = [node for node in env_nodes if gen_node.name == node.name and node.name]
            if len(wrong_nodes) > 0:
                if gen_node.flops != wrong_nodes[0].flops:
                    name_idx = 0
                    new_name = res.name + "_node_" + str(name_idx)
                    while new_name in (env_names + used_nodes):
                        name_idx += 1
                        new_name = res.name + "_node_" + str(name_idx)
                    gen_node.name = new_name
            temp_nodes.append(gen_node)

        res.nodes = temp_nodes

        filled_power = sum(s.flops for s in res.nodes)
        if filled_power > fc:
            print("================= wrong chromosome at the start of mutate phase " + str(filled_power))

        if len(res.nodes) == 0:
            pass


##===================================
## GA specie
##==================================

def ga_default_initialize(ctx, size):
    """
    chromosome representation changed from (task, node_idx) to (task, blade_idx, node_idx)
    """
    env = ctx['env']

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
        temp = []
        for t in chromo:
            resources = env.rm.get_live_resources()
            res = [resource for resource in resources][random.randint(0, len(resources) - 1)]
            # TODO may be need use res_amount, later
            node = [elem for elem in res.nodes][random.randint(0, len(res.nodes) - 1)]
            temp.append((t.id, res.name, node.name))
        ls = ListBasedIndividual(temp)
        result.append(ls)

    return result


def vm_random_count_generate(ctx):
    env = ctx['env']
    result = []
    for res in env.rm.resources:
        fc = res.farm_capacity
        mrc = res.max_resource_capacity
        max_sweep_size = env.wf.get_max_sweep() / 2
        currentCap = 0
        n = -1
        while currentCap < fc - mrc and n < max_sweep_size:
            n += 1
            tmp_capacity = random.randint(0, mrc)
            currentCap += tmp_capacity
        if currentCap < fc and n < max_sweep_size:
            n += 1
        result.append(random.randint(0, n))
    return result

def ga_mutate(ctx, mutant):
    k = len(mutant) / 25
    env = ctx['env']
    wf = env.wf
    env_nodes = [node.name for node in env.rm.resources[0].nodes]
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

            used_resources = []
            for c in mutant:
                if c[1] not in used_resources:
                    used_resources.append(c[1])
            cell = random.randint(0, len(mutant) - 1)
            res = used_resources[random.randint(0, len(used_resources) - 1)]
            used_nodes = [c[2] for c in mutant if c[1] == res] + env_nodes
            node = used_nodes[random.randint(0, len(used_nodes) - 1)]
            mutant[cell] = (mutant[cell][0], res, node)
    if random.random() < k / (2 * len(mutant)):
        used_resources = []
        for c in mutant:
            if c[1] not in used_resources:
                used_resources.append(c[1])
        for i in range(len(mutant)):
            if random.random() < k / len(mutant):
                k1 = random.randint(0, len(mutant) - 1)
                res_number = used_resources[random.randint(0, len(used_resources) - 1)]
                used_nodes = [c[2] for c in mutant if c[1] == res_number] + env_nodes
                if len(used_nodes) == 0:
                    continue
                node_number = used_nodes[random.randint(0, len(used_nodes) - 1)]
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