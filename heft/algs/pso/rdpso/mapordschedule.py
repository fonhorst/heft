from deap.base import Fitness
from heft.algs.common.NewSchedulerBuilder import place_task_to_schedule
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import Utility


MAPPING_SPECIE = "MappingSpecie"
ORDERING_SPECIE = "OrderingSpecie"

from heft.algs.pso.rdpso.rdpso import mappingTransform

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

    assert check_precedence(workflow, os), "Precedence is violated"

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
        sched = build_schedule(wf, estimator, rm, position)

    # isvalid = Utility.is_static_schedule_valid(wf,sched)
    # if not isvalid:
    #     print("NOT VALID SCHEDULE!")

    makespan = Utility.makespan(sched)
    ## TODO: make a real estimation later
    cost = 0.0
    Fitness.weights = [-1.0, -1.0]
    fit = Fitness(values=(makespan, cost))
    ## TODO: make a normal multi-objective fitness estimation
    fit.mofit = makespan
    return fit



def mapping_from_schedule(schedule, mapMatrix):
    mapping = {item.job.id: mapMatrix[item.job.id, node.name] for node, items in schedule.mapping.items()
               for item in items}
    return mapping


def ordering_from_schedule(schedule):
    ordering = sorted((item for node, items in schedule.mapping.items() for item in items),
                      key=lambda x: x.start_time)
    ordering = [item.job.id for item in ordering]
    return ordering


def ord_and_map(schedule, mapMatrix):
    return mapping_from_schedule(schedule, mapMatrix), ordering_from_schedule(schedule)


def validate_mapping_with_alive_nodes(mapping, rm, mapMatrix):
    """
    :param mapping: is a dict {(task_id):(node_name)}
    :param rm: resource manager
    :return:
    """
    #TODO transform mapping from task:runtime to task:node
    mapping_tr = mappingTransform(mapping, mapMatrix, rm.get_nodes())

    alive_nodes = [node.name for node in rm.get_nodes() if node.state != Node.Down]
    for task_id, node_name in mapping_tr.items():
        if node_name not in alive_nodes:
            return False
    return True



