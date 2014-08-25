from deap.base import Fitness
from heft.algs.common.NewSchedulerBuilder import place_task_to_schedule
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import Utility

MAPPING_SPECIE = "MappingSpecie"
ORDERING_SPECIE = "OrderingSpecie"


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


def _check_precedence(workflow, seq):
    for i in range(len(seq)):
        task = workflow.byId(seq[i])
        pids = [p.id for p in task.parents]
        for j in range(i + 1, len(seq)):
            if seq[j] in pids:
                return False
    return True


def fitness(wf, rm, estimator, position):
    sched = build_schedule(wf, estimator, rm, position)
    makespan = Utility.makespan(sched)
    ## TODO: make a real estimation later
    cost = 0.0
    Fitness.weights = [-1.0, -1.0]
    fit = Fitness(values=(makespan, cost))
    ## TODO: make a normal multi-objective fitness estimation
    fit.mofit = makespan
    return fit


def mapping_from_schedule(schedule):
    mapping = {item.job.id: node.name for node, items in schedule.mapping.items() for item in items}
    return mapping


def ordering_from_schedule(schedule):
    ordering = sorted((item for node, items in schedule.mapping.items() for item in items),
                      key=lambda x: x.start_time)
    ordering = [item.job.id for item in ordering]
    return ordering
