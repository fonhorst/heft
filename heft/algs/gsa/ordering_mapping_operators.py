import functools
import operator
import random

from heft.algs.common.particle_operations import MappingParticle, Particle
from heft.algs.common.utilities import cannot_be_zero
from heft.algs.pso.mapping_operators import position_update


def force(p, pop, kbest, G):
    def mutual_force(p1, p2):
        """
        estimate force between p1 and p2
        p must be a Particle with overriden operators
        """
        diff = p1 - p2
        dist = 1#diff.vector_length()
        #print("dist: {0}".format(dist))
        dist = cannot_be_zero(dist)
        f_abs = G * (p1.mass * p2.mass) / dist
        force = diff * f_abs
        return force

    pop = sorted(pop, key=lambda x: x.mass)
    active_elements = pop[0:kbest]
    # if p in active_elements:
    #     return p.emptify()
    forces = [mutual_force(p, a) for a in active_elements]
    common_force = functools.reduce(operator.add, forces)
    return common_force


def mapping_update(w, c, p):
    acceleration = p.force/p.mass
    alpha = random.random()
    new_velocity = p.velocity*w + acceleration*c*alpha
    new_entity = position_update(p, new_velocity)
    p.entity = new_entity
    p.velocity = new_velocity
    pass


def ordering_update(w, c, p, min=-1, max=1):
    acceleration = p.force/p.mass
    alpha = random.random()
    new_velocity = p.velocity*w + acceleration*c*alpha
    new_position = (p + new_velocity)
    new_position.limit_by(min, max)
    p.entity = new_position.entity
    pass


class CompoundParticle(Particle):
    def __init__(self, mapping_particle, ordering_particle):
        super().__init__(None)
        self.mapping = mapping_particle
        self.ordering = ordering_particle
        self._best = None

        self._mass = None
        self._force = None

        pass

    def _get_mass(self):
        return self._mass

    def _set_mass(self, value):
        self._mass = value
        self.mapping.mass = self._mass
        self.ordering.mass = self._mass
        pass

    def _get_force(self):
        return self._force

    def _set_force(self, value):
        self._force = value
        mapping_force, ordering_force = value
        self.mapping.force = mapping_force
        self.ordering.force = ordering_force
        pass


    mass = property(_get_mass, _set_mass)
    force = property(_get_force, _set_force)
    pass

