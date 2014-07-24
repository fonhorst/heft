import functools
import random

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

def run_gsa(toolbox, statistics, logbook, pop_size, iter_number, kbest, ginit):
    """
    This method is targeted to propose a prototype implementation of
    Gravitational Search Algorithm(gsa). It is intended only for initial steps
    of algorithm understanding and shouldn't be considered for real applications

    We need specify the following enities:
    1) Vector representation
    2) generate() - generation of initial solutions
    3) fitness(p) (with mofit field)
    4) mass(p, worst, best)
    5) force_vector_matrix(pop, kbest, G)
    6) position(p, velocity) - function of getting new position
    7) G(g, i) - changing of G-constant, where i - a number of a current iteration
    8) kbest(kbest, i) - changing of kbest
    """

    G = ginit
    kbest_init = kbest

    ## initialization
    ## generates random solutions
    pop = [toolbox.generate() for _ in range(pop_size)]

    for i in range(iter_number):
        ## fitness estimation
        for p in pop:
            ## toolbox.fitness must return Fitness
            p.fitness = toolbox.fitness(p)
        ## mass estimation
        ## It is assumed that a minimization task is solved
        ## special field 'mofit' is used to get complex value of fitness
        pop = sorted(pop, key=lambda x: x.fitness.mofit)
        best = pop[0].fitness.mofit
        worst = pop[-1].fitness.mofit
        for p in pop:
            p.mass = 1 + (p.fitness.mofit - worst) / (1 if abs(best - worst) < 0.001 else (best - worst))
        mass_sum = sum(p.mass for p in pop)
        for p in pop:
            p.mass = p.mass/mass_sum

        ## estimate all related forces
        ## fvm is a matrix of VECTORS(due to the fact we are operating in d-dimensional space) size of 'pop_size x kbest'
        ## in fact we can use wrapper for the entity of pop individual but python has duck typing,
        ## so why don't use it, if you use it carefully?
        fvm = toolbox.force_vector_matrix(pop, kbest, G)


        ##statistics gathering
        record = statistics.compile(pop)
        logbook.record(gen=i, G=G, kbest=kbest, **record)
        print(logbook.stream)

        ## compute new velocity and position
        position = toolbox.position if hasattr(toolbox, 'position') else None
        pop = [toolbox.velocity_and_position(p, fvm, position) for p in pop]

        ## change gravitational constants
        G = toolbox.G(ginit, i, iter_number)
        kbest = toolbox.kbest(kbest_init, kbest, i, iter_number)

        ##removing temporary elements
        for p in pop:
            if hasattr(p, 'mass'): del p.mass
            if hasattr(p, 'fitness'): del p.fitness
            if hasattr(p, 'acceleration'): del p.acceleration
        pass
    return pop


