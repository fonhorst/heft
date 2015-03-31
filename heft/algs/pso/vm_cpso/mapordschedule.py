from deap.base import Fitness
from copy import deepcopy
from heft.algs.common.NewSchedulerBuilder import NewScheduleBuilder
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import Utility
import random

MAPPING_SPECIE = "MappingSpecie"
ORDERING_SPECIE = "OrderingSpecie"


def particle_converter(ms, os, node_map):
    """
    Convert ms and os to one particle {node:[task]}
    :param ms: [taskId, nodeName]
    :param os: {taskId]
    :return:{node:[tasks]}
    """
    res_part = dict()
    for node in node_map.keys():
        res_part[node] = []
    for ord_it in os:
        map_it = [m for m in ms if m[0] == ord_it][0]
        res_part[map_it[1]].append(map_it[0])
    return res_part

def merge_rms(old, new):
    """
    Merge complete RM with init RM and with new RM from particle
    :param old: init RM
    :param new: RM in particle
    :return: new Merged RM with all nodes
    """
    rm = deepcopy(old)
    for res in new.resources:
        if res.name not in [elem.name for elem in rm.resources]:
            rm.resources.append(res)
        else:
            for node in res.nodes:
                rm_res = rm.get_res_by_name(res.name)
                if node.name not in [elem.name for elem in rm_res.nodes]:
                    rm_res.nodes.append(node)
    return rm

def build_schedule(workflow, estimator, resource_manager, fixed_schedule, current_time, solution, res_particle):
    """
    build schedule with merged RM and with fixed schedule
    :param resource_manager: init RM
    :param solution: ms = [taskId, nodeName] os = [taskId]
    :param res_particle: new RM without all nodes in RM
    """
    ms = solution[MAPPING_SPECIE]
    os = solution[ORDERING_SPECIE]

    #assert check_precedence(workflow, os), "Precedence is violated"

    # Merge init ResourceManger with config particle
    rm = merge_rms(resource_manager, res_particle.entity)

    # check all map items, if resource not found in res_particle, change to random resource
    for map_item in ms:
        node = res_particle.entity.get_node_by_name(map_item[1])
        if node is None:
            live_nodes = [l_node for l_node in res_particle.entity.get_live_nodes()]
            if len(live_nodes) == 0:
                raise Exception("Live_nodes lenght == 0")
            node = live_nodes[random.randint(0, len(live_nodes) - 1)]
            bad_idx = ms.index(map_item)
            new_map_item = (map_item[0], node.name)
            ms[bad_idx] = new_map_item

    # make task and node maps
    task_map = {}
    node_map = {}

    for task in workflow.get_all_unique_tasks():
        task_map[task.id] = task

    for node in rm.get_all_nodes():
        node_map[node.name] = node

    particle = particle_converter(ms, os, node_map)

    builder = NewScheduleBuilder(workflow, rm, estimator, task_map, node_map, fixed_schedule)

    schedule = builder(particle, current_time)
    return schedule


def check_precedence(workflow, task_seq):
    for i in range(len(task_seq)):
        task = workflow.byId(task_seq[i])
        pids = [p.id for p in task.parents]
        for j in range(i + 1, len(task_seq)):
            if task_seq[j] in pids:
                return False
    return True


def fitness(wf, rm, estimator, position):
    if isinstance(position, Schedule):
        sched = position
    else:
        raise Exception("sched is incorrect")

    makespan = Utility.makespan(sched)
    ## TODO: make a real estimation later
    cost = 0.0
    Fitness.weights = [-1.0, -1.0]
    fit = Fitness(values=(makespan, cost))
    ## TODO: make a normal multi-objective fitness estimation
    fit.mofit = makespan
    return fit



def mapping_from_schedule(schedule):
    mapping = {item.job.id: node.name for node, items in schedule.mapping.items()
               for item in items}
    return mapping


def ordering_from_schedule(schedule):
    ordering = sorted((item for node, items in schedule.mapping.items() for item in items),
                      key=lambda x: x.start_time)
    ordering = [item.job.id for item in ordering]
    return ordering


def ord_and_map(schedule):
    return mapping_from_schedule(schedule), ordering_from_schedule(schedule)


def validate_mapping_with_alive_nodes(mapping, rm):
    """
    :param mapping: is a dict {(task_id):(node_name)}
    :param rm: resource manager
    :return:
    """
    #TODO transform mapping from task:runtime to task:node

    alive_nodes = [node.name for node in rm.get_nodes() if node.state != Node.Down]
    for task_id, node_name in mapping.items():
        if node_name not in alive_nodes:
            return False
    return True



