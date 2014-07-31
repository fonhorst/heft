"""
This is a prototype of set-based PSO algorithm directed to solve workflow scheduling problem with fixed ordering.
It must be refactored and made in reusable, configurable form later.

Position =>{task_name: node_name}
Velocity => {(task_name, node_name): probability}
"""
from _ctypes import ArgumentError
from copy import deepcopy
from numbers import Number
import random
from deap import tools
from deap import creator
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.common.mapordschedule import fitness as basefitness


class FrozenDict(dict):
    # def __setitem__(self, key, value):
    #     raise ValueError("Operation is not allowed for this type")
    pass


class Position(FrozenDict):
    def __init__(self, d):
        super().__init__(d)

    def __sub__(self, other):
        # return Position({k: self[k] for k in self.keys() - other.keys()})
        return Velocity({item: 1.0 for item in self.items() - other.items()})

    def __mul__(self, other):
        if isinstance(other, Number):
            return Velocity({k: other for k, v in self.items()})
        raise ArgumentError("Other has not a suitable type for multiplication")
    pass


class Velocity(FrozenDict):

    def __init__(self, d):
        super().__init__(d)

    def __mul__(self, other):
        if isinstance(other, Number):
            return Velocity({k: 1 if v * other > 0 else v * other for k, v in self.items()})
        raise ArgumentError("{0} has not a suitable type for multiplication".format(other))

    def __add__(self, other):
        return Velocity({k: max(self.get(k, 0), other.get(k, 0)) for k in set(self.keys()).union(other.keys())})

    def cutby(self, alpha):
        return Velocity({k: v for k, v in self.items() if v >= alpha})

    pass


creator.create("Particle", base=FitAdapter, velocity=None, best=None)
Particle = creator.Particle


def _cutting_by_task(velocity, task):
    return [FitAdapter(node, (v,)) for (task, node), v in velocity.items()]

def velocity_update(w, c1, c2, pbest, gbest, velocity, position):
    r1 = random.random()
    r2 = random.random()
    new_velocty = velocity*w + (pbest - position)*(c1*r1) + (gbest - position)*(c2*r2)
    return new_velocty

def position_update(position, velocity):
    alpha = random.random()
    cut_velocity = velocity.cutby(alpha)
    new_position = {}
    for task in position:
        available_nodes = _cutting_by_task(cut_velocity, task)
        if len(available_nodes) == 0:
            available_nodes = [FitAdapter(position[task], (1.0,))]
        new_node = tools.selRoulette(available_nodes, 1)[0].entity
        new_position[task] = new_node
    return Position(new_position)




def run_pso(w, c1, c2, gen, n, toolbox, stats, logbook):
    """
    :param w:
    :param c1:
    :param c2:
    :param gen:
    :param n:
    :param toolbox:
    :param stats:
    :param logbook:
    :return:

    for toolbox we need the following functions:
    population
    fitness
    update

    And the following params:
    w
    c1
    c2
    """
    pop = toolbox.population(n)
    # stats = tools.Statistics(lambda ind: ind.fitness.values)
    # stats.register("avg", numpy.mean)
    # stats.register("std", numpy.std)
    # stats.register("min", numpy.min)
    # stats.register("max", numpy.max)

    # logbook = tools.Logbook()
    # logbook.header = ["gen", "evals"] + stats.fields

    best = None

    for g in range(gen):
        for p in pop:
            p.fitness = toolbox.fitness(p)
            if not p.best or p.best.fitness < p.fitness:
                p.best = deepcopy(p)
            if not best or best.fitness < p.fitness:
                best = deepcopy(p)

        # Gather all the fitnesses in one list and print the stats
        data = stats.compile(pop) if stats is not None else None
        if logbook is not None:
            logbook.record(gen=g, evals=len(pop), **data)
            print(logbook.stream)

        for p in pop:
            toolbox.update(w, c1, c2, p, best)
    return pop, logbook, best


def schedule_to_position(schedule):
    return Particle(Position({item.job.id: node.name for node, items in schedule.mapping.items() for item in items}))


def update(w, c1, c2, p, best):
    p.velocity = velocity_update(w, c1, c2, p.best.entity, best.entity, p.velocity, p.entity)
    new_position = position_update(p.entity, p.velocity)
    p.entity = new_position
    pass


def generate(wf, rm, estimator, n):
    pop = []
    for i in range(n):
        sched = SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule()
        particle = schedule_to_position(sched)
        particle.velocity = Velocity({})
        pop.append(particle)
    return pop

def construct_solution(position, sorted_tasks):
    return {MAPPING_SPECIE: [(t, position[t]) for t in sorted_tasks], ORDERING_SPECIE: sorted_tasks}

def fitness(wf, rm, estimator, sorted_tasks, particle):
    position = particle.entity
    solution = construct_solution(position, sorted_tasks)
    return basefitness(wf, rm, estimator, solution)





