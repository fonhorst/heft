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
import distance
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.common.mapordschedule import fitness as basefitness
from heft.experiments.cga.utilities.common import hamming_distances


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
            return Velocity({k: 1.0 if v * other > 1.0 else v * other for k, v in self.items()})
        raise ArgumentError("{0} has not a suitable type for multiplication".format(other))

    def __add__(self, other):
        vel = Velocity({k: max(self.get(k, 0), other.get(k, 0)) for k in set(self.keys()).union(other.keys())})
        return vel

    def cutby(self, alpha):
        return Velocity({k: v for k, v in self.items() if v >= alpha})

    pass


creator.create("Particle", base=FitAdapter, velocity=None, best=None)
Particle = creator.Particle


def _cutting_by_task(velocity, task):
    return [FitAdapter(node, (v,)) for (task, node), v in velocity.items()]

def velocity_update(w, c1, c2, pbest, gbest, velocity, position, pop):
    # c3, c4, tasks, nodes
    r1 = random.random()
    r2 = random.random()
    # r3 = random.random()
    # r4 = random.random()
    # def choose_random_pair(tasks, nodes):
    #     t = tasks[random.randint(0, len(tasks) - 1)]
    #     n = nodes[random.randint(0, len(tasks) - 1)]
    #     return (t, n)

    #pcuriosity = Position({t: n for t, n in [choose_random_pair(tasks, nodes) for _ in range(0, 2)]})
    #localbest = max((p for p in pop if 0 < distance.hamming(position, pop) <=2), key=lambda x: x.fitness)
    old_velocity = velocity*w
    pbest_velocity = (pbest - position)*(c1*r1)
    gbest_velocity = (gbest - position)*(c2*r2)

    new_velocty = old_velocity + pbest_velocity + gbest_velocity #+ (localbest - position)*(c3*r3) + pcuriosity*(c4*r4)
    # print("===== new velocity: {0}".format(new_velocty))
    return new_velocty

def position_update(position, velocity):
    alpha = random.random()
    cut_velocity = velocity.cutby(alpha)
    new_position = {}
    for task in position:
        available_nodes = _cutting_by_task(cut_velocity, task)
        if len(available_nodes) == 0:
            available_nodes = [FitAdapter(position[task], (1.0,))]

        #print("=== task: {0}; available nodes: {1}".format(task, [node.entity for node in available_nodes]))

        # new_node = tools.selRoulette(available_nodes, 1)[0].entity
        # new_node = max(available_nodes, key=lambda x: x.fitness).entity
        # new_node = tools.selTournament(available_nodes, 1, 2)[0].entity
        new_node = available_nodes[random.randint(0, len(available_nodes) - 1)].entity
        new_position[task] = new_node
    return Position(new_position)

def position_update2(position, velocity):
    alpha = random.random()
    cut_velocity = velocity.cutby(alpha)
    new_position = {}
    for task, node in position.items():
        new_position[task] = node

    tasks = list(position.keys())
    task = tasks[random.randint(0, len(tasks) - 1)]

    available_nodes = _cutting_by_task(cut_velocity, task)
    if len(available_nodes) == 0:
        available_nodes = [FitAdapter(position[task], (1.0,))]

    #print("=== task: {0}; available nodes: {1}".format(task, [node.entity for node in available_nodes]))

    # new_node = tools.selRoulette(available_nodes, 1)[0].entity
    #  new_node = max(available_nodes, key=lambda x: x.fitness).entity
    # new_node = tools.selTournament(available_nodes, 1, 2)[0].entity
    new_node = available_nodes[random.randint(0, len(available_nodes) - 1)].entity
    new_position[task] = new_node
    return Position(new_position)


## TODO: remove it a little bit later, after the implementation of run_pso will show its suitable applicability
# class MappingPSO:
#     def __init__(self, w, c1, c2, gen, n, toolbox, stats, logbook):
#         self._w = w
#         self._c1 = c1
#         self._c2 = c2
#         self._gen = gen
#         self._n = n
#         self._toolbox = toolbox
#         self._stats = stats
#         self._logbook = logbook
#
#         self._current_gen = 0
#         self._pop = toolbox.population(n)
#         self._best = None
#         pass
#
#     def __call__(self):
#         if self._gen is None:
#             raise ValueError("Generations count is not valid")
#         for g in range(self._gen):
#             self._current_gen = g
#             self.evolve(gen_curr=g, gen_step=1, pop=self._pop)
#         return self.result()
#
#     def evolve(self, gen_curr, gen_step=1, pop=None):
#         if pop is not None:
#             self._pop = pop
#
#         for g in range(gen_curr, gen_curr + gen_step):
#             for p in self._pop:
#                 if not hasattr(p, "fitness") or not p.fitness.valid:
#                     p.fitness = self._toolbox.fitness(p)
#                 if not p.best or p.best.fitness < p.fitness:
#                     p.best = deepcopy(p)
#                 if not self._best or self._best.fitness < p.fitness:
#                     self._best = deepcopy(p)
#
#             # Gather all the fitnesses in one list and print the stats
#             data = self._stats.compile(self._pop) if self._stats is not None else None
#             if self._logbook is not None:
#                 self._logbook.record(gen=g, evals=len(self._pop), **data)
#                 print(self._logbook.stream)
#
#             for p in self._pop:
#                 self._toolbox.update(self._w, self._c1, self._c2, p, self._best, self._pop)
#         return self.result()
#
#     def result(self):
#         return self._pop, self._logbook, self._best


def run_pso(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, pop=None, **params):

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
    n
    """
    # pso = MappingPSO(w, c1, c2, gen, n, toolbox, stats, logbook)
    # return pso()

    w, c1, c2, n = params["w"], params["c1"], params["c2"], params["n"]
    ## TODO: remove it later
    #w = w * (500 - gen_curr)/500

    best = params.get('best', None)

    if pop is None:
        pop = toolbox.population(n)

    for g in range(gen_curr, gen_curr + gen_step, 1):
        for p in pop:
            if not hasattr(p, "fitness") or not p.fitness.valid:
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
            toolbox.update(w, c1, c2, p, best, pop)

        if invalidate_fitness:
            for p in pop:
                del p.fitness
        pass
    return pop, logbook, best






def schedule_to_position(schedule):
    return Particle(Position({item.job.id: node.name for node, items in schedule.mapping.items() for item in items}))


def update(w, c1, c2, p, best, pop):
    p.velocity = velocity_update(w, c1, c2, p.best.entity, best.entity, p.velocity, p.entity, pop)
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





