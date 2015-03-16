import functools
import operator
from distance import hamming
from heft.algs.common.particle_operations import MappingParticle
from heft.algs.common.utilities import cannot_be_zero
from heft.algs.pso.mapping_operators import update


def mapping_force_vector_matrix(pop, kbest, G):
    """
    :return: dictionary of p_uid with array of associated influential powers
    """
    def force(p1, p2):
        """
        estimate force between p1 and p2
        p must be a Position
        """
        ## TODO: remake it later
        dist = hamming(p1.entity, p2.entity)
        dist = cannot_be_zero(dist)
        f_abs = G * (p1.mass * p2.mass) / dist
        force = {item: p * f_abs for item, p in (p1.entity - p2.entity).items()}
        return MappingParticle.Velocity(force)

    pop = sorted(pop, key=lambda x: x.mass)
    active_elements = pop[0:kbest]
    forces = [[force(p, a) for a in active_elements] for p in pop]
    return forces


def mapping_velocity_and_position(p, fvm, position_func, beta=1.0):
    forces = fvm[p.uid]
    force = functools.reduce(operator.add, forces)
    acceleration = force / p.mass
    new_velocity = p.velocity*beta + acceleration
    new_position = update(p.entity, new_velocity)
    new_particle = MappingParticle(new_position)
    new_particle.velocity = new_velocity
    return new_particle




