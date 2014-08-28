from numbers import Number


class Position(dict):
    def __init__(self, d):
        super().__init__(d)

    def __sub__(self, other):
        # return Position({k: self[k] for k in self.keys() - other.keys()})
        return Velocity({item: 1.0 for item in self.items() - other.items()})

    def __mul__(self, other):
        if isinstance(other, Number):
            return Velocity({k: other for k, v in self.items()})
        raise ValueError("Other has not a suitable type for multiplication")
    pass


class Velocity(dict):

    def __init__(self, d):
        super().__init__(d)

    def __mul__(self, other):
        if isinstance(other, Number):
            if other < 0:
                raise ValueError("Only positive numbers can be used for operations with velocity")
            return Velocity({k: 1.0 if v * other > 1.0 else v * other for k, v in self.items()})
        raise ValueError("{0} has not a suitable type for multiplication".format(other))

    def __add__(self, other):
        vel = Velocity({k: max(self.get(k, 0), other.get(k, 0)) for k in set(self.keys()).union(other.keys())})
        return vel

    def __truediv__(self, denumenator):
       if isinstance(denumenator, Number):
           return self.__mul__(denumenator)
       raise ValueError("{0} has not a suitable type for division".format(denumenator))

    def cutby(self, alpha):
        return Velocity({k: v for k, v in self.items() if v >= alpha})

    pass
