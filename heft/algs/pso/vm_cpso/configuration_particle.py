import random
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.algs.pso.vm_cpso.particle_operations import ConfigurationParticle
from heft.algs.pso.vm_cpso.mapping_operators import velocity_update
from heft.core.environment.BaseElements import Resource, Node, SoftItem, Workflow
from copy import deepcopy

def config_generate(rm):
    """
    generate random resources in order to max flops for res and in total
    """
    config = []
    for res in rm.resources:
        cur_config = []
        fc = res.farm_capacity
        mrc = res.max_resource_capacity
        cur_cap = 0
        while cur_cap < fc - mrc:
            temp_cap = random.randint(1, mrc)
            cur_cap += temp_cap
            cur_config.append(temp_cap)
        if cur_cap < fc:
            cur_config.append((fc - cur_cap))
        new_res = flops_to_resource(res, cur_config)
        config.append(new_res)
    new_rm = deepcopy(rm)
    new_rm.resources = config
    return ConfigurationParticle(new_rm)

def flops_to_resource(res, flops):
    """
    Make resourse with set of nodes, provided from flops
    if node have same flops with node in init RM, copy this node from init RM
    :param res: cur Resource
    :param flops: list of flops [10, 15, 25, 30]
    :return: new Resource with nodes, whom flops equal to flops list
    """
    nodes = []
    live_nodes = res.get_live_nodes()
    cemetery = res.get_cemetery()
    used_nodes = []
    for cap in flops:
        possible_nodes = [node for node in live_nodes if node.flops == cap and node.name not in used_nodes]
        if len(possible_nodes) > 0:
            new_node = deepcopy(possible_nodes[random.randint(0, len(possible_nodes) - 1)])
            used_nodes.append(new_node.name)
        else:
            cur_idx = 0
            cur_name = res.name + "_node_" + str(cur_idx)
            while cur_name in ([node.name for node in live_nodes] + [node.name for node in cemetery] + used_nodes):
                cur_idx += 1
                cur_name = res.name + "_node_" + str(cur_idx)
            used_nodes.append(cur_name)
            new_node = Node(cur_name, res, [SoftItem.ANY_SOFT], cap)
        nodes.append(new_node)
    new_res = deepcopy(res)
    new_res.nodes = nodes
    return new_res

def normalization(particle, init_rm):
    res = init_rm.resources[0]
    mrc = res.max_resource_capacity
    fc = res.farm_capacity
    particle = [round(flops, 0) for flops in particle if flops >= 1]
    res_particle = []
    cur_capacity = 0
    for p in particle:
        if p > 30:
            pass
    for flops in particle:
        if cur_capacity + flops > fc:
            break
        while flops > mrc:
            res_particle.append(mrc)
            flops -= mrc
            cur_capacity += mrc
        if flops > 0:
            res_particle.append(flops)
            cur_capacity += flops
    if len(res_particle) == 0:
        res_particle = [node.flops for node in res.get_live_nodes()]
        if len(res_particle) == 0:
            new_config = config_generate(init_rm)
            res_particle = [node.flops for node in new_config.entity.resources[0].get_live_nodes()]
    for node in res_particle:
        if node > 30:
            pass
    return res_particle

def configuration_update(w, c1, c2, p, best, init_rm):
    new_velocity = velocity_update(w, c1, c2, p.best, best, p.velocity, p)
    new_entity = normalization((p + new_velocity), init_rm)
    new_res = flops_to_resource(init_rm.resources[0], new_entity)
    p.entity.resources = [new_res]
    p.velocity = new_velocity
    pass


