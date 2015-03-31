import random
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.pso.vm_cpso.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.pso.vm_cpso.particle_operations import MappingParticle
from heft.algs.pso.vm_cpso.mapordschedule import fitness as basefitness
from heft.algs.common.utilities import weight_random


def position_update(mapping_particle, velocity):
    # TODO if value riched 1, than change node
    def _cutting_by_task(velocity, cur_task):
        # return [node for (task, node), v in velocity.items()]
        return [(node, v) for (task, node), v in velocity.items() if task == cur_task]
    alpha = random.random()
    cut_velocity = velocity.cutby(alpha)
    new_position = {}
    for task in mapping_particle.entity:
        available_nodes = _cutting_by_task(cut_velocity, task)
        if len(available_nodes) == 0:
            available_nodes = [(mapping_particle.entity[task], 1)]
            # available_nodes = [mapping_particle.entity[task]]

        new_node = available_nodes[weight_random([v for k, v in available_nodes])]
        # new_node = available_nodes[random.randint(0, len(available_nodes) - 1)]
        new_position[task] = new_node[0]
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

def velocity_update(w, c1, c2, pbest, gbest, velocity, particle):
        r1 = random.random()
        r2 = random.random()

        old_velocity = velocity*w
        pbest_velocity = (pbest - particle)*(c1*r1)
        gbest_velocity = (gbest - particle)*(c2*r2)

        new_velocity = old_velocity + pbest_velocity + gbest_velocity
        return new_velocity