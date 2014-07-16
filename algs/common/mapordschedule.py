from algs.common.NewSchedulerBuilder import place_task_to_schedule
from core.environment.ResourceManager import Schedule

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
