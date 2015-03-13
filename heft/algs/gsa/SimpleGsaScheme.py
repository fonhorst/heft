from copy import deepcopy
import functools
import operator
import random
from heft.algs.common.utilities import cannot_be_zero, gather_info
from heft.core.environment.Utility import profile_decorator


def _randvecsum(vectors):
    l = len(vectors[0])
    val = [_randsum(v[i] for v in vectors) for i in range(l)]
    return val

def _randsum(iterable):
    add = lambda a, b: a + random.random()*b
    return functools.reduce(add, iterable)

def calculate_velocity_and_position(p, fvm, estimate_position):
    ## get vector of total influential force for all dimensions
    total_force = _randvecsum(vec for vec in fvm[p])
    p.acceleration = [f/p.mass for f in total_force]
    # p.velocity = toolbox.velocity(p, velocity, acceleration)
    p.velocity = random.random()*p.velocity + p.acceleration
    p = estimate_position(p)
    return p

def run_gsa(toolbox, stats, logbook, n, gen_curr, gen_step, initial_pop=None, kbest=None, ginit=None, **params):
    """
    This method is targeted to propose a prototype implementation of
    Gravitational Search Algorithm(gsa). It is intended only for initial steps
    of algorithm understanding and shouldn't be considered for real applications

    We need specify the following enities:
    1) Vector representation
    2) generate() - generation of initial solutions
    3) fitness(p) (with mofit field)
    4) mass(p, worst, best)
    5) estimate_force(p, pop, kbest, G)
    6) update(p) - function of getting new position
    7) G(g, i) - changing of G-constant, where i - a number of a current iteration
    8) kbest(kbest, i) - changing of kbest
    9) w - inertia koeff
    """

    G = ginit
    kbest_init = kbest

    ## initialization
    ## generates random solutions
    pop = toolbox.generate(n) if initial_pop is None else initial_pop

    best = None

    for i in range(gen_curr, gen_curr + gen_step):
        ## fitness estimation
        for p in pop:
            ## toolbox.fitness must return Fitness
            p.fitness = toolbox.fitness(p)
        ## mass estimation
        ## It is assumed that a minimization task is solved
        pop = sorted(pop, key=lambda x: x.fitness)
        best_fit = pop[0].fitness
        worst_fit = pop[-1].fitness
        # TODO: this is a hack
        max_diff = best_fit.values[0] - worst_fit.values[-1]
        max_diff = cannot_be_zero(max_diff)
        for p in pop:
            p.mass = cannot_be_zero((p.fitness.values[0] - worst_fit.values[0]) / max_diff)
        ## convert to (0, 1) interval
        ## TODO: perhaps this should 'warn' message
        # mass_sum = cannot_be_zero(sum(p.mass for p in pop))
        # for p in pop:
        #     p.mass = p.mass/mass_sum

        ## TODO: only for debug. remove it later.
        #print(functools.reduce(operator.add, (" " + str(round(p.mass, 4)) for p in pop), ""))


        ## estimate all related forces
        ## fvm is a matrix of VECTORS(due to the fact we are operating in d-dimensional space) size of 'pop_size x kbest'
        ## in fact we can use wrapper for the entity of pop individual but python has duck typing,
        ## so why don't use it, if you use it carefully?
        for p in pop:
            p.force = toolbox.estimate_force(p, pop, kbest, G)

        ##statistics gathering
        ## TODO: replace it back
        data = stats.compile(pop) if stats is not None else None
        if logbook is not None:
            logbook.record(gen=i, kbest=kbest, G=G, evals=len(pop), **data)
            print(logbook.stream)
        # gather_info(logbook, stats, i, pop, need_to_print=True)

        new_best = max(pop, key=lambda x: x.fitness)
        if best is None:
            best = deepcopy(new_best)
        else:
            best = deepcopy(max(best, new_best, key=lambda x: x.fitness))
        ## compute new velocity and position
        for p in pop:
            toolbox.update(p)
        # position = toolbox.position if hasattr(toolbox, 'position') else None
        # pop = [toolbox.velocity_and_position(p, forces, position) for p, f in zip(pop, fvm)]

        ## change gravitational constants
        G = toolbox.G(ginit, i, gen_step)
        kbest = toolbox.kbest(kbest_init, kbest, i, gen_step)

        ##removing temporary elements
        for p in pop:
            if hasattr(p, 'fitness'): del p.fitness
            if hasattr(p, 'acceleration'): del p.acceleration
        pass
    return pop, logbook, best


