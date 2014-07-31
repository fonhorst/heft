"""
This is a prototype of set-based PSO algorithm directed to solve workflow scheduling problem with fixed ordering.
It must be refactored and made in reusable, configurable form later.

Position =>{task_name: node_name}
Velocity => {(task_name, node_name): probability}
"""
from _ctypes import ArgumentError
from numbers import Number
import random
from deap import tools
from heft.algs.common.individuals import FitAdapter


class FrozenDict(dict):
    def __setitem__(self, key, value):
        raise ValueError("Operation is not allowed for this type")


class Position(FrozenDict):
    def __init__(self, d):
        super(d)

    def __sub__(self, other):
        return Position({k: self[k] for k in self.keys() - other.keys()})

    def __mul__(self, other):
        if isinstance(other, Number):
            return Velocity({k: other for k, v in self.items()})
        raise ArgumentError("Other has not a suitable type for multiplication")
    pass


class Velocity(FrozenDict):

    def __init__(self, d):
        super(d)

    def __mul__(self, other):
        if isinstance(other, Number):
            return Velocity({k: 1 if v * other > 0 else v * other for k, v in self.items()})
        raise ArgumentError("{0} has not a suitable type for multiplication".format(other))

    def __add__(self, other):
        return Velocity({k: max(self.get(k, 0), other.get(k, 0)) for k in self.keys() + other.keys()})

    def cutby(self, alpha):
        return Velocity({k: v for k, v in self.items() if v >= alpha})

    pass


def _cutting_by_task(velocity, task):
    return [FitAdapter(node, (v,)) for task, node, v in velocity.items()]

def velocity_update(w, c1, c2, pbest, gbest, velocity, position):
    r1 = random.random()
    r2 = random.random()
    new_velocty = w*velocity + (c1*r1)*(pbest - position) + (c2*r2)*(gbest - position)
    return new_velocty

def position_update(position, velocity):
    alpha = random.random()
    cut_velocity = velocity.cutby(alpha)
    new_position = {}
    for task in position:
        available_nodes = _cutting_by_task(cut_velocity, task)
        if len(available_nodes) == 0:
            available_nodes = position[task]
        new_node = tools.selRoulette(available_nodes, 1).entity
        new_position[task] = new_node
    return new_position


def run_pso():
    raise NotImplementedError()




