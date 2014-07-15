import functools
import operator
import random

def _randvecsum(vectors):
    l = len(vectors[0])
    val = [_randsum(v[i] for v in vectors) for i in range(l)]
    return val

def _randsum(iterable):
    add = lambda a, b: a + random.random()*b
    return functools.reduce(add, iterable)

def run_gsa(toolbox, pop_size, iter_number, kbest, ginit):
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
        pop = sorted(pop, key=p.fitness.mofit)
        best = pop[0]
        worst = pop[-1]
        for p in pop:
            p.mass = toolbox.mass(p, worst, best)
        mass_sum = sum(p for p in pop)
        for p in pop:
            p.mass = p.mass/mass_sum

        ## estimate all related powers
        ## pvm is a matrix of VECTORS(due to we are operating in d-dimensional space) size of 'pop_size x kbest'
        ## in fact we can use wrapper for the entity of pop individual but python has duck typing,
        ## so why don't use it, if you use it carefully?
        pvm = toolbox.force_vector_matrix(pop, kbest, G)
        ## compute new velocity and position
        for p in pop:
            ## get vector of total influential force for all dimensions
            total_force = _randvecsum(vec for vec in pvm[p])
            p.acceleration = [f/p.mass for f in total_force]
            # p.velocity = toolbox.velocity(p, velocity, acceleration)
            velocity = random.random()*p.velocity + p.acceleration
            p = toolbox.position(p, velocity)
            p.velocity = velocity

        ## change gravitational constants
        G = toolbox.G(G, i)
        kbest = toolbox.kbest(kbest, i)


        for p in pop:
            del p.mass
            del p.fitness
            del p.acceleration
        pass
    pass


