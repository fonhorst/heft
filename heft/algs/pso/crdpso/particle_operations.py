from numbers import Number
from uuid import uuid4
import math
from heft.algs.common.individuals import FitAdapter


class Particle(FitAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uid = uuid4()
        self._velocity = None
        self._best = None

    def _get_best(self): return self._best
    def _set_best(self, b): self._best = b

    def _get_velocity(self): return self._velocity
    def _set_velocity(self, v): self._velocity = v

    best = property(_get_best, _set_best)
    velocity = property(_get_velocity, _set_velocity)
    pass



class MappingParticle(Particle):
    def __init__(self, mapping):
        super().__init__(mapping)
        self.velocity = MappingParticle.Velocity({})
    pass

    def __sub__(self, other):
        # return Position({k: self[k] for k in self.keys() - other.keys()})
        return MappingParticle.Velocity({item: 1.0 for item in self.entity.items()# - other.entity.items()
        })

    def __mul__(self, other):
        if isinstance(other, Number):
            return MappingParticle.Velocity({k: other for k, v in self.entity.items()})
        raise ValueError("Other has not a suitable type for multiplication")

    def emptify(self):
        return MappingParticle.Velocity({})

    class Velocity(dict):
        def __mul__(self, other):
            if isinstance(other, Number):
                if other < 0:
                    raise ValueError("Only positive numbers can be used for operations with velocity")
                return MappingParticle.Velocity({k: 1.0 if v * other > 1.0 else v * other for k, v in self.items()})
            raise ValueError("{0} has not a suitable type for multiplication".format(other))

        def __add__(self, other):
            vel = MappingParticle.Velocity({k: max(self.get(k, 0), other.get(k, 0)) for k in set(self.keys()).union(other.keys())})
            return vel

        def __truediv__(self, denumenator):
           if isinstance(denumenator, Number):
               return self.__mul__(1/denumenator)
           raise ValueError("{0} has not a suitable type for division".format(denumenator))

        def cutby(self, alpha):
            return MappingParticle.Velocity({k: v for k, v in self.items() if v >= alpha})

        def vector_length(self):
            return len(self)


        pass
    pass


class OrderingParticle(Particle):

    def __init__(self, ordering):
        super().__init__(ordering)

    def __sub__(self, other):
        if not isinstance(other, OrderingParticle):
            raise ValueError("Invalid type of the argument for this operation")
        velocity = OrderingParticle.Velocity([self.entity[iter][1] - other.entity[iter][1]
                                              for iter in range(len(self.entity))])
        return velocity

    def __add__(self, other):
        if not isinstance(other, OrderingParticle.Velocity):
            raise ValueError("Invalid type of the argument for this operation: {0}".format(type(other)))
        new_pos = OrderingParticle([(self.entity[iter][0], self.entity[iter][1] + other[iter])
                                              for iter in range(len(self.entity))])
        return new_pos

    class Velocity(list):

        def __mul__(self, other):
            if not isinstance(other, Number):
                raise ValueError("{0} has not a suitable type for multiplication".format(other))
            if other < 0:
                raise ValueError("Only positive numbers can be used for operations with velocity")
            return OrderingParticle.Velocity([round(v * other, 0) for v in self])


        def __add__(self, other):
            if not isinstance(other, OrderingParticle.Velocity):
                raise ValueError("{0} has not a suitable type for adding".format(other))
            if (len(self) == 0):
                vel = other
            else:
                vel = OrderingParticle.Velocity([self[iter] + other[iter] for iter in range(len(self))])
            return vel
    pass


class CompoundParticle(Particle):
    def __init__(self, mapping_particle, ordering_particle):
        super().__init__(None)
        self.mapping = mapping_particle
        self.ordering = ordering_particle
        self._best = None
        pass

    def _get_best(self):
        return self._best

    def _set_best(self, value):
        self._best = value
        if value is not None:
            self.mapping.best = value.mapping
            self.ordering.best = value.ordering
        else:
            self.mapping.best = None
            self.ordering.best = None
        pass

    best = property(_get_best, _set_best)
    pass

class ConfigurationParticle(Particle):
    def __init__(self, config):
        super().__init__(config)
        self.velocity = ConfigurationParticle.Velocity({})

    def emptify(self):
        return ConfigurationParticle.Velocity({})

    def __sub__(self, other):
        if not isinstance(other, ConfigurationParticle):
            raise ValueError("Invalid type of the argument for this operation")
        vel = ConfigurationParticle.Velocity({key: (val - other.entity[key]) for key, val in self.entity.items()})
        return vel

    def get_nodes(self):
        nodes = [(k, v) for k, v in self.entity.items()]
        nodes.sort(key=lambda k: k[0])
        return [v for k, v in nodes]

    def __add__(self, other):
        new_pos = ConfigurationParticle({k: v + other[k] for k, v in self.entity.items()})
        return new_pos

    class Velocity(dict):
        def __mul__(self, other):
            vel = ConfigurationParticle.Velocity({k: v * other for k, v in self.items()})
            return vel

        def __add__(self, other):
            if len(self) == 0:
                vel = other
            else:
                vel = ConfigurationParticle.Velocity({k: (v + other[k]) for k, v in self.items()})
            return vel





