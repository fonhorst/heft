import random
from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter
from heft.algs.common.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE, ord_and_map, build_schedule as base_build_schedule
from heft.algs.common.setbasedoperations import Position
from heft.algs.common.utilities import gather_info
from heft.algs.common.mapordschedule import fitness as basefitness
from heft.algs.pso.sdpso import Particle
from heft.core.environment.Utility import Utility


class CompoundParticle(FitAdapter):
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
        self.mapping.best = value.mapping
        self.ordering.best = value.ordering
        pass

    best = property(_get_best, _set_best)
    pass
## it is succesful version and should be deleted later,
## but for the first it is preferable to get sure It definetely will not become required
# def run_ompso(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, initial_pop=None, **params):
#     """
#     fusion of pso algorithms targeted to dealing with mapping and ordering for workflow scheduling
#     toolbox must contain:
#     generate,
#     fitness,
#     pso_mapping,
#     pso_ordering,
#     VNS if any
#     """
#
#     #W, C1, C2 = params["w"], params["c1"], params["c2"]
#
#     # mappings = generate_initial_mappings(N) #toolbox
#     # orderings = generate_initial_orderings(N) #toolbox
#     # pop = combine(mappings, orderings) #toolbox
#
#     N = len(initial_pop) if initial_pop is not None else params["N"]
#
#     pop = initial_pop if initial_pop is not None else toolbox.generate(N)
#
#     best = None
#     for g in range(gen_curr, gen_curr + gen_step):
#
#         for p in pop:
#             p.fitness = toolbox.fitness(p)
#
#         best = max(pop, key=lambda x: x.fitness)
#
#         gather_info(logbook, stats, g, pop)
#
#         # toolbox and **params are already partially applied
#         pop_pr, _, _ = toolbox.pso_mapping(logbook=None, stats=None,
#                                            gen_curr=g, gen_step=1,
#                                            invalidate_fitness=False, initial_pop=pop,
#                                            best=best)
#
#         pop_pr, _, _ = toolbox.pso_ordering(logbook=None, stats=None,
#                                             gen_curr=g, gen_step=1,
#                                             invalidate_fitness=False, initial_pop=pop,
#                                             best=best)
#
#         if hasattr(toolbox, "VNS") and toolbox.VNS is not None:
#             pop_pr, _, _ = toolbox.VNS(logbook=None, stats=None,
#                                        gen_curr=g, gen_step=1,
#                                        invalidate_fitness=False, initial_pop=pop,
#                                        best=best)
#
#         if invalidate_fitness:
#             for p in pop:
#                 del p.fitness
#
#         pass
#     return pop, logbook, best



def construct_solution(position, sorted_tasks):
    return {MAPPING_SPECIE: [(t, position[t]) for t in sorted_tasks], ORDERING_SPECIE: sorted_tasks}


def build_schedule(wf, rm, estimator, particle):

    def recover_ordering(ordering):
        corrected_ordering = []

        while len(ordering) > 0:
            ord_iter = iter(ordering)
            while True:
                t, v = next(ord_iter)
                if Utility.is_enough_to_be_executed(wf, t, corrected_ordering):
                    ordering.remove((t, v))
                    corrected_ordering.append(t)
                    break
                pass
            pass
        return corrected_ordering

    sorted_tasks = wf.get_tasks_id()
    ordering = sorted(zip(sorted_tasks, particle.ordering.entity), key=lambda x: x[1])

    ordering = recover_ordering(ordering)

    solution = construct_solution(particle.mapping.entity, ordering)
    sched = base_build_schedule(wf, estimator, rm, solution)
    return sched


def fitness(wf, rm, estimator, particle):
    sched = build_schedule(wf, rm, estimator, particle)
    return basefitness(wf, rm, estimator, sched)


def ordering_to_numseq(ordering, min=-1, max=1):
    step = abs((max - min)/len(ordering))
    sorted_tasks = sorted(ordering)
    initial = min
    ord_position = []
    for job_id in ordering:
        initial += step
        ord_position.append(initial)
    return ord_position


def generate(wf, rm, estimator, schedule=None, fixed_schedule_part=None, current_time=0.0):
    sched = schedule if schedule is not None else SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule(fixed_schedule_part, current_time)
    mapping, ordering = ord_and_map(sched)
    ordering = ordering_to_numseq(ordering)
    ord_p, map_p = Particle(ordering), Particle(Position(mapping))
    return CompoundParticle(map_p, ord_p)


def ordering_update(w, c1, c2, p, best, pop, min=-1, max=1):

    def limit_velocity(velocity):
        v = [(el - min)/abs(el - min) * abs(max - min) if abs(el - min) > abs(max - min) else el for el in velocity]
        return v

    def limit_position(position):
        pos = [max if p > max else min if p < min else p for p in position]
        return pos

    def diff(a, b):
        d = [ael - bel for ael, bel in zip(a,b)]
        return d

    def mul(vector, a):
        d = [el * a for el in vector]
        return d

    def add(a, b):
        d = [a + b for ael, bel in zip(a,b)]
        return d


    # Particle here
    r1 = random.random()
    r2 = random.random()
    new_velocity = add(add(mul(p.velocity, w), mul(diff(p.entity, p.best.entity), c1*r1)), mul(diff(p.entity, best.entity), c2*r2))
    new_velocity = limit_velocity(new_velocity)
    p.entity = limit_position(p.entity + new_velocity)
    p.velocity = new_velocity
    return p
