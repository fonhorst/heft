from copy import deepcopy
from math import exp
import random
from deap import creator
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE, fitness


creator.create("State", base=object, energy=None, mapping=None, ordering=None)
State = creator.State


def energy(wf, rm, estimator, state):
    position = {MAPPING_SPECIE: [item for item in state.mapping.items()],
                ORDERING_SPECIE:state.ordering}
    return fitness(wf, rm, estimator, position)


def update_T(T0, T, N, g):
    return T0 - ((T0/N) * g)


def mapping_neighbor(wf, rm, estimator, count, state):
    """
    it takes current mapping, randomly chooses count: elements
    and replaces it with new elements
    :param count: count of genes which will be changed
    :param state:
    :return:
    """

    if count > len(state.mapping):
        raise ValueError("count is greater than mapping")
    mappings = list(state.mapping.items())
    for_changes = []
    while len(for_changes) < count:
        new_ri = random.randint(0, len(mappings) - 1)
        if new_ri not in for_changes:
            for_changes.append(new_ri)
        pass

    def move_task(taskid, node_name):
        nodes = list(rm.get_nodes())
        if len(nodes) < 2:
            return node_name
        while True:
            n = random.randint(0, len(nodes) - 1)
            return nodes[n].name

    num_seq = range(len(mappings))
    try_to_move = lambda el, num: move_task(el[0], el[1]) if (num in for_changes) else el[1]
    new_mappings = {el[0]: try_to_move(el, num) for el, num in zip(mappings, num_seq)}

    new_state = deepcopy(state)
    new_state.mapping = new_mappings
    return new_state


def transition_probability(current_state, new_state, T):
    if new_state.energy > current_state.energy:
        return 1
    diff = new_state.energy.values[0] - current_state.energy.values[0]
    return exp(-diff/T)


