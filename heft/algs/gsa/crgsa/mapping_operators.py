import random
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.gsa.crgsa.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.gsa.crgsa.particle_operations import MappingParticle
from heft.algs.gsa.crgsa.mapordschedule import fitness as basefitness


"""
def position_update(mapping_particle, velocity):
    def _cutting_by_task(velocity, cur_task):
        return [node for (task, node), v in velocity.items()]
        #return [node for (task, node), v in velocity.items() if task == cur_task]
    alpha = random.random()
    cut_velocity = velocity.cutby(alpha)
    new_position = {}
    for task in mapping_particle.entity:
        available_nodes = _cutting_by_task(cut_velocity, task)
        if len(available_nodes) == 0:
            available_nodes = [mapping_particle.entity[task]]

        new_node = available_nodes[random.randint(0, len(available_nodes) - 1)]#.entity
        new_position[task] = new_node
    return new_position
"""

def position_update(mapping_particle, velocity):
    def _cutting_by_task(velocity, cur_task):
        cut = [(node, v) for (task, node), v in velocity.items() if task == cur_task]
        return [node for node, v in cut], [v for node, v in cut]
    new_position = {}
    for task in mapping_particle.entity:
        nodes, values = _cutting_by_task(velocity, task)
        if len(nodes) == 0:
            nodes = [mapping_particle.entity[task]]
            values = [1]

        new_node = nodes[weight_random(values)]
        new_position[task] = new_node
    return new_position


def schedule_to_position(schedule):
    return MappingParticle({item.job.id: node.name for node, items in schedule.mapping.items() for item in items})

def generate(wf, rm, estimator, n):
    pop = []
    for i in range(n):
        sched = SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule()
        particle = schedule_to_position(sched)
        particle.velocity = MappingParticle.Velocity({})
        pop.append(particle)
    return pop


def construct_solution(particle, sorted_tasks):
    return {MAPPING_SPECIE: [(t, particle.entity[t]) for t in sorted_tasks], ORDERING_SPECIE: sorted_tasks}


def fitness(wf, rm, estimator, sorted_tasks, particle):
    solution = construct_solution(particle, sorted_tasks)
    return basefitness(wf, rm, estimator, solution)

def weight_random(list):
    summ = sum(list)
    norm = [v / summ for v in list]
    rnd = random.random()
    idx = 0
    stack = norm[idx]
    while rnd >= stack:
        idx += 1
        stack += norm[idx]
    return idx