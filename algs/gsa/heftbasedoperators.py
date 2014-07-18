from deap import tools
import math
from deap.base import Fitness
from algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from algs.common.individuals import DictBasedIndividual
from algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from algs.common.utilities import mapping_as_vector
from algs.gsa.operators import fitness as basefitness


## TODO: remake vector representation -> sorted
def schedule_to_position(schedule):
    """
    this function extracts only mapping from a schedule
    because position is considered only as a mapping
    """
    items = lambda: iter((item, node) for node, items in schedule.mapping.items() for item in items)
    if not all(i.is_unstarted() for i, _ in items()):
        raise ValueError("Schedule is not valid. Not all elements have unstarted state.")
    mapping = DictBasedIndividual({i.job.id: n.name for i, n in items()})
    return mapping

def generate(wf, rm, estimator):
    sched = SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule()
    return schedule_to_position(sched)

def force_vector_matrix(rm, pop, kbest, G, e=0.0):
    """
    returns matrix of VECTORS size of 'pop_size*kbest'
    distance between two vectors is estimated with hamming distance
    Note: pop is sorted by decreasing of goodness, so to find kbest
    we only need to take first kbest of pop
    """
    sub = lambda seq1, seq2: [0 if s1 == s2 else 1 for s1, s2 in zip(seq1, seq2)]
    zero = lambda: [0 for _ in range(len(pop[0]))]
    dist = lambda a, b: sum([(0 if r1 == r2 else 1) + math.fabs(rm.byName(r1).flops - rm.byName(r2).flops)/(rm.byName(r1).flops + rm.byName(r2).flops) for r1, r2 in zip(a, b)])

    def estimate_force(a, b):
        a_string = mapping_as_vector(a)
        b_string = mapping_as_vector(b)

        R = dist(a_string, b_string)
        ## TODO: here must be a multiplication of a vector and a number
        val = (G*(a.mass*b.mass)/R + e)
        f = [val * d for d in sub(a_string, b_string)]
        return f

    mat = {p.uid: [(zero(), b) if p == b else (estimate_force(p, b), b) for b in pop[0:kbest]] for p in pop}
    return mat

def velocity_and_position(wf, rm, estimator, p, fvm, estimate_position=None):

    def change(d):
        ## this function returns new state for dimension d
        ## depending on acting forces of other masses
        class TempWrapper:
            def __init__(self, pd, dacceleration):
                self.pd = pd
                self.fitness = Fitness(values=(dacceleration,))
            pass

        ## get all forces which act from all other participating masses to mass p
        ## for all vectors of force save force value and point in discrete dimension where it is
        dforces = [TempWrapper(mapping_as_vector(mass)[d], f[d]/p.mass) for f, mass in fvm[p.uid]]

        ## case without changing of current place in space
        ## acts like yet another divicion for roulette
        not_changing = sum([mass.mass for _, mass in fvm[p.uid]])/(p.mass*len(fvm[p.uid]))
        if not_changing < 1:
            dforces.append(TempWrapper(mapping_as_vector(p)[d], sum([x.fitness.values[0] for x in dforces]) * not_changing))

        el = tools.selRoulette(dforces, 1)[0]
        return el.pd
    ## construct new position vector based on forces
    new_p = [change(i) for i in range(len(p))]
    return new_p

def fitness(wf, rm, estimator, ordering, position):
    solution = {MAPPING_SPECIE: position.items(), ORDERING_SPECIE: ordering}
    fit = basefitness(wf, rm, estimator, solution)
    return fit


