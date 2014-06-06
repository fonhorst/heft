from GA.DEAPGA.coevolution.operators import fitness_mapping_and_ordering, MAPPING_SPECIE, ORDERING_SPECIE, build_schedule
from core.concrete_realization import ExperimentEstimator
from environment.BaseElements import Resource, Node, SoftItem


class VMResGen:
    @staticmethod
    def r(list_types, n):
        result = []
        res = Resource("res_0")
        possible_configs = [t for t in list_types for i in range(n)]
        for flop, i in zip(possible_configs, range(len(possible_configs))):
            node = Node(res.name + "_node_" + str(i), res, [SoftItem.ANY_SOFT])
            node.flops = flop
            result.append(node)
        res.nodes = result
        return [res]
    pass

class SimpleTimeCostEstimator(ExperimentEstimator):
    def __init__(self, comp_time_cost, transf_time_cost, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.comp_time_cost = comp_time_cost
        self.transf_time_cost = transf_time_cost
    def computation_cost(self, task, node):
        time = self.estimate_runtime(task, node)
        return time * self.comp_time_cost
    def transfer_cost(self, node1, node2, task1, task2):
        time = self.estimate_transfer_time(node1, node2, task1, task2)
        return time * self.transf_time_cost
    pass

def cost(schedule, estimator):
    tn = schedule.task_to_node()
    ccost = sum(estimator.computation_cost(t, n) for t, n in tn.items())
    tcost = sum(sum(estimator.transfer_cost(n, tn[p], t, p) for p in t.parents if not p.is_head) for t, n in tn.items())
    return ccost + tcost


def cost_mapping_and_ordering(ctx, solution):
    env = ctx['env']
    schedule = build_schedule(env.wf, env.estimator, env.rm, solution)
    all_cost = cost(schedule, env.estimator)
    return -all_cost


def fitness_makespan_and_cost_map_ord(ctx, x):
    if isinstance(x, dict):
        sol = x
    else:
        sol = {MAPPING_SPECIE: x[0], ORDERING_SPECIE: x[1]}
    makespan = fitness_mapping_and_ordering(ctx, sol)
    cost = cost_mapping_and_ordering(ctx, sol)
    ## TODO: to correct this issue we need to refactor single-objective cga version
    return -makespan, -cost

