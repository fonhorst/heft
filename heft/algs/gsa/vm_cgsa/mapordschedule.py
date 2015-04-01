from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import Utility
from deap.base import Fitness


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
    """
    alive_nodes = [node.name for node in rm.get_nodes() if node.state != Node.Down]
    for task_id, node_name in mapping.items():
        if node_name not in alive_nodes:
            return False
    return True



