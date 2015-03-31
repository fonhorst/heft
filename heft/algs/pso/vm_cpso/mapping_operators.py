import random
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.pso.cpso.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.pso.cpso.particle_operations import MappingParticle
from heft.algs.pso.cpso.csdpso import velocity_update
from heft.algs.pso.cpso.mapordschedule import fitness as basefitness


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


def schedule_to_position(schedule):
    return MappingParticle({item.job.id: node.name for node, items in schedule.mapping.items() for item in items})


def update(w, c1, c2, p, best):
    p.velocity = velocity_update(w, c1, c2, p.best, best, p.velocity, p)
    new_position = position_update(p, p.velocity)
    p.entity = new_position
    pass


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