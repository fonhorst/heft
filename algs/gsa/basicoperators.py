"""
1) Vector representation
2) generate() - generation of initial solutions
3) fitness(p) (with mofit field)
4) mass(p, worst, best)
5) force_vector_matrix(pop, kbest, G)
6) position(p, velocity) - function of getting new position
7) G(g, i) - changing of G-constant, where i - a number of a current iteration
8) kbest(kbest, i) - changing of kbest
"""
from deap.base import Fitness


class Position:
    def __init__(self, mapping=None, ordering=None):
        self.mapping = mapping
        self.ordering = ordering
    pass

def generate():
    pass

def fitness(position):
    raise NotImplementedError()
    fit = Fitness()
    return fit

def mass(position, worst, best):
    raise NotImplementedError()
    ms = None
    return ms

def force_vector_matrix(pop, kbest, G):
    raise NotImplementedError()
    mat = None
    return mat

def position(position, velocity):
    raise NotImplementedError()
    new_position = None
    return new_position

def G(g, i):
    raise NotImplementedError()
    ng = None
    return ng

def Kbest(kbest, i):
    raise NotImplementedError()
    nkbest = None
    return nkbest

