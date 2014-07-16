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
import math
from algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from algs.gsa.utilities import schedule_to_position
from core.environment.Utility import Utility
from experiments.cga.utilities.common import hamming_distances


class Position(dict):
    def __init__(self, mapping=None, ordering=None):
        super().__setitem__(MAPPING_SPECIE, mapping)
        super().__setitem__(ORDERING_SPECIE, ordering)

    def as_vector(self):
        """
        converts position to a single vector by joining mapping and ordering structure
        all workflow tasks is sorted by task.id and thus it provides idempotentity for multiple runs
        """
        mapp_string = [node_name for task_id, node_name in sorted(super().__getitem__(MAPPING_SPECIE), key=lambda x: x[0])]
        ord_string = super().__getitem__[ORDERING_SPECIE]
        return mapp_string + ord_string

    def __setitem__(self, key, value):
        raise RuntimeError("This operation is not permitted for this data structure")

    def __delitem__(self, key):
        raise RuntimeError("This operation is not permitted for this data structure")

    def __len__(self):
        return len(self.__getitem__(MAPPING_SPECIE)) + len(self.__getitem__(ORDERING_SPECIE))
    pass

def generate(wf, rm, estimator):
    sched = SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule()
    return schedule_to_position(sched)

def fitness(wf, rm, estimator, position):
    sched = build_schedule(wf, estimator, rm, position)
    makespan = Utility.makespan(sched)
    ## TODO: make a real estimation later
    cost = 0.0
    fit = Fitness(values=(makespan, cost))
    ## TODO: make a normal multi-objective fitness estimation
    fit.mofit = makespan
    return fit

def force_vector_matrix(pop, kbest, G, e=0.0):
    """
    returns matrix of VECTORS size of 'pop_size*kbest'
    distance between two vectors is estimated with hamming distance
    Note: pop is sorted by decreasing of goodness, so to find kbest
    we only need to take first kbest of pop
    """
    sub = lambda seq1, seq2: [0 if s1 == s2 else 1 for s1, s2 in zip(seq1, seq2)]
    zero = lambda: [0 for _ in range(len(pop[0]))]

    def estimate_force(a, b):
        a_string = a.as_vector()
        b_string = b.as_vector()

        R = hamming_distances(a_string, b_string)
        ## TODO: here must be a multiplication of a vector and a number
        val = (G*(a.mass*b.mass)/R + e)
        f = [val * d for d in sub(a_string, b_string)]
        return f

    mat = [[zero() if p == b else estimate_force(p, b) for b in pop[0:kbest]] for p in pop]
    return mat

def position(wf, rm, estimator, position, velocity):
    ## TODO: rework it in an elegant way
    raise NotImplementedError()
    threshold = 0.4
    new_vector = [change(d) if vd > threshold else d for vd, d in zip(velocity, position.as_vector())]
    new_position = Position.from_vector(new_vector)
    return new_position

def G(ginit, i, iter_number):
    ng = ginit*(1 - i/iter_number)
    return ng

def Kbest(kbest, i, iter_number):
    """
    basic implementation of kbest decreasing
    """
    nkbest = math.ceil(i / iter_number)
    return nkbest

