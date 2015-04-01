import random
import math
from copy import deepcopy
from deap.base import Fitness
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.common.utilities import mapping_as_vector
from heft.core.environment.Utility import Utility
from heft.experiments.cga.utilities.common import hamming_distances
from heft.algs import SimpleRandomizedHeuristic
from heft.algs.common.NewSchedulerBuilder import NewScheduleBuilder


def generate(wf, rm, estimator):
    sched = SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule()
    return schedule_to_position(sched)



def force_vector_matrix(pop, kbest, G, e=0.0):
    """
    returns matrix of VECTORS size of 'pop_size*kbest'
    distance between two vectors is estimated with hamming distance
    Note: pop is sorted by decreasing of goodness, so to find kbest
    we only need to take first kbest of pop
    """
    sub = lambda seq1, seq2: [0 if s1 == s2 else 1 for s1, s2 in zip(seq1, seq2)]
    zero = lambda: [0 for _ in range(len(pop[0]))]

    def estimate_force(a, b):
        a_string = a.as_vector()
        b_string = b.as_vector()

        R = hamming_distances(a_string, b_string)
        ## TODO: here must be a multiplication of a vector and a number
        val = (G*(a.mass*b.mass)/R + e)
        f = [val * d for d in sub(a_string, b_string)]
        return f

    mat = [[zero() if p == b else estimate_force(p, b) for b in pop[0:kbest]] for p in pop]
    return mat

def position(wf, rm, estimator, position, velocity):
    ## TODO: do normal architecture of relations in the first place
    ## TODO: rework it in an elegant way
    raise NotImplementedError()
    unchecked_tasks = wf.get_all_unique_tasks()
    def change(d):
        if d.startswith("ID"):
            s = set(node.name for node in rm.get_nodes())
            s.remove(d)
            s = list(s)
            new_name = d if len(s) == 0 else s[random.randint(0, len(s) - 1)]
        else:

            s = set(t.id for t in tasks)
            s.remove(d)
            s = [el for el in s if not el.checked]
            ## TODO: add condition for checking of precedence
            if len(s) == 0:
                ## TODO: check case
                new_name = d
            else:
                while len(s) > 0:
                    el = s[random.randint(0, len(s) - 1)]
                    task = wf.byId(el)
                    if all(is_checked(el) for p in task.parents):
                        task.checked = True
                        new_name = el
                        break
                    else:
                        s.remove(el)
        pass
    threshold = 0.4
    new_vector = [change(d) if vd > threshold else d for vd, d in zip(velocity, position.as_vector())]
    new_position = Position.from_vector(new_vector)
    return new_position

def G(ginit, i, iter_number, all_iter_number=None):
    ng = ginit*(1 - i/iter_number) if all_iter_number is None else ginit*(1 - i/all_iter_number)
    return ng

def Kbest(kbest_init, kbest, i, iter_number, all_iter_number=None):
    """
    basic implementation of kbest decreasing
    """
    iter_number = iter_number if all_iter_number is None else all_iter_number
    d = iter_number / kbest_init
    nkbest = math.ceil(abs(kbest_init - i/d))
    return nkbest


def schedule_to_position(schedule):
    """
    this function converts valid schedule
    to mapping and ordering strings
    """
    items = lambda: iter((item, node) for node, items in schedule.mapping.items() for item in items)
    if not all(i.is_unstarted() for i, _ in items()):
        raise ValueError("Schedule is not valid. Not all elements have unstarted state.")

    mapping = {i.job.id: n.name for i, n in items()}
    ordering = sorted([i for i, _ in items()], key=lambda x: x.start_time)
    ordering = [el.job.id for el in ordering]
    return Position(mapping, ordering)

def particles_adapter(sched, config):
    """
    During init and update process, some assigned nodes in mapping not existed in config.
    Therefore, it is required to change this nodes according to nodes from current config.
    """
    nodes = [node for node in config.get_live_nodes()]
    nodes_names = [node.name for node in nodes]
    new_mapping = dict()
    for task, node in sched.mapping.entity.items():
        if node not in nodes_names:
            new_mapping[task] = nodes[random.randint(0, len(nodes) - 1)].name
        else:
            new_mapping[task] = node
    sched.mapping.entity = new_mapping

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

    # Merge init ResourceManger with config particle
    rm = merge_rms(resource_manager, res_particle.entity)

    # check all map items, if resource not found in res_particle
    for map_item in ms:
        node = res_particle.entity.get_node_by_name(map_item[1])
        if node is None:
            raise Exception("Paticles are not compatible")

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

