import functools
import operator
import random

from heft.algs.gsa.crgsa.particle_operations import MappingParticle, Particle, OrderingParticle, ConfigurationParticle
from heft.algs.common.utilities import cannot_be_zero
from heft.algs.gsa.crgsa.mapping_operators import position_update
from heft.algs.gsa.crgsa.ordering_operators import generate as pso_generate, ordering_transform
from numbers import Number


def force(p, pop, kbest, G):
    def mutual_force(p1, p2):
        """
        estimate force between p1 and p2
        p must be a Particle with overriden operators
        """
        diff = p2 - p1
        dist = 1#diff.vector_length()
        #print("dist: {0}".format(dist))
        dist = cannot_be_zero(dist)
        f_abs = G * (p1.mass * p2.mass) / (dist)
        force = diff * f_abs
        return force

    pop = sorted(pop, key=lambda x: x.mass, reverse=False)
    active_elements = pop[0:kbest]
    if p in active_elements:
        return p.emptify()
    forces = [mutual_force(p, a) for a in active_elements]
    common_force = functools.reduce(operator.add, forces)
    return common_force



def mapping_update(w, c, p):
    acceleration = p.force/(p.mass)
    alpha = random.random()
    new_velocity = p.velocity*w + acceleration*c*alpha
    new_entity = position_update(p, new_velocity)
    p.entity = new_entity
    p.velocity = new_velocity
    available_items = {(k, v) for k, v in p.entity.items()}
    p.velocity = MappingParticle.Velocity({item: v for item, v in new_velocity.items() if item in available_items})
    pass


def ordering_update(w, c, p, rank_list):
    if len(p.force) != 0 and not isinstance(p.force[0], Number):
        p.force = OrderingParticle.Velocity([item[1] for item in p.force])
    acceleration = p.force/(p.mass)
    alpha = random.random()
    new_velocity = p.velocity*w + acceleration*c*alpha
    new_position = (p + new_velocity)
    new_position = ordering_transform(new_position.entity, rank_list)
    p.entity = new_position
    p.velocity = new_velocity
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


def generate(wf, rm, estimator, rank_list, schedule=None, fixed_schedule_part=None, current_time=0.0):
    particle = pso_generate(wf, rm, estimator, rank_list, schedule, fixed_schedule_part, current_time)
    particle = CompoundParticle(particle.mapping, particle.ordering)
    return particle

