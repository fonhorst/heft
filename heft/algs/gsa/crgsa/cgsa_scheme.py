from copy import deepcopy
import functools
import operator
import numpy
import math
import random
from heft.algs.common.utilities import cannot_be_zero, gather_info
import deap
from heft.core.environment.Utility import profile_decorator
from heft.algs.heft.HeftHelper import HeftHelper
from functools import partial
from heft.algs.common.utilities import gather_info


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

def run_cgsa(toolbox, stats, logbook, n, gen_curr, gen_step, initial_pop=None, kbest=None, ginit=None, **params):
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

    pop = toolbox.generate(n) if initial_pop is None else initial_pop
    pop2 = toolbox.config_population(n)

    best = None

    hall_of_fame = []
    hall_size = int(math.log(n))
    best_idx = 0
    change_chance = 0.1


    for i in range(gen_curr, gen_curr + gen_step):



        if i == 0:
            leaders, winner = gamble(pop, pop2, n, toolbox, toolbox.standard())
        else:
            leaders, winner = gamble(pop, pop2, n, toolbox)

        p1_leader = leaders[random.randint(0, len(leaders)-1)][0][0]
        p2_leader = leaders[random.randint(0, len(leaders)-1)][0][1]

        for p in pop:
            p.fitness = toolbox.fitness(p, p2_leader)
        for p in pop2:
            p.fitness = toolbox.fitness(p1_leader, p)

        #best update
        winner[0][0].fitness = winner[1]

        p1_best = max(pop, key=lambda x: x.fitness)
        p2_best = max(pop2, key=lambda x: x.fitness)
        p1_best = ((p1_best, p2_leader), p1_best.fitness)
        p2_best = ((p1_leader, p2_best), p2_best.fitness)
        if best is None:
            best = deepcopy(max(p1_best, p2_best, winner, key=lambda x: x[1]))
        else:
            best = deepcopy(max(p1_best, p2_best, best, winner, key=lambda x: x[1]))



        if len(hall_of_fame) < hall_size or hall_of_fame[-1][1] < best[1]:
                hall_of_fame = change_hall(hall_of_fame, best, hall_size)

        best_idx = change_index(best_idx, change_chance, len(hall_of_fame))
        best = hall_of_fame[best_idx]

        best[0][0].mass = 0.00005
        best[0][1].mass = 0.00005
        #print(str(i) + "\t" + "BEST: fitness = " + str(best[1]) + "\t" + "nodes: " + str(best[0][1].get_nodes()))

        ##statistics gathering
        gather_info(logbook, stats, i, (pop+pop2+[winner[0][0]]), hall_of_fame[0], True)

        ## mass estimation
        ## It is assumed that a minimization task is solved
        pop = sorted(pop, key=lambda x: x.fitness)
        pop2 = sorted(pop2, key=lambda x: x.fitness)
        best_fit = pop[0].fitness
        worst_fit = pop[-1].fitness
        best_fit2 = pop2[0].fitness
        worst_fit2 = pop2[-1].fitness
        max_diff = best_fit.values[0] - worst_fit.values[0]
        max_diff = cannot_be_zero(max_diff)
        max_diff2 = best_fit2.values[0] - worst_fit2.values[0]
        max_diff2 = cannot_be_zero(max_diff2)

        """
        for p in pop:
            p.mass = cannot_be_zero((best_fit.values[0] - p.fitness.values[0]) / max_diff)
        for p in pop2:
            p.mass = cannot_be_zero((best_fit2.values[0] - p.fitness.values[0]) / max_diff2)
        """
        for p in pop:
            p.mass = cannot_be_zero((p.fitness.values[0] - worst_fit.values[0]) / max_diff)
        for p in pop2:
            p.mass = cannot_be_zero((p.fitness.values[0] - worst_fit2.values[0]) / max_diff2)


        ## estimate all related forces
        ## fvm is a matrix of VECTORS(due to the fact we are operating in d-dimensional space) size of 'pop_size x kbest'
        ## in fact we can use wrapper for the entity of pop individual but python has duck typing,
        ## so why don't use it, if you use it carefully?
        for p in pop:
            p.force = toolbox.estimate_force(p, pop+[best[0][0]], kbest, G)
        for p in pop2:
            p.force = toolbox.estimate_config_force(p, pop2+[best[0][1]], kbest, G)



        ## compute new velocity and position
        for p in pop:
            toolbox.update(p)
        for p in pop2:
            toolbox.config_update(p)

        ## change gravitational constants
        G = toolbox.G(ginit, i, gen_step)
        kbest = toolbox.kbest(kbest_init, kbest, i, gen_step)
        #print("G = " + str(G) + "\t" + "kbest = " + str(kbest))

        ##removing temporary elements
        for p in pop:
            if hasattr(p, 'fitness'): del p.fitness
            if hasattr(p, 'acceleration'): del p.acceleration
        for p in pop2:
            if hasattr(p, 'fitness'): del p.fitness
            if hasattr(p, 'acceleration'): del p.acceleration
        pass
    hall_of_fame.sort(key=lambda p: p[1], reverse=True)
    best = hall_of_fame[0]

    return pop, logbook, best

def gamble(pop1, pop2, n, toolbox, fix_solution=None):
    games = {}
    if fix_solution is not None:
        games[fix_solution] = toolbox.fitness(fix_solution[0], fix_solution[1])
    for _ in range(n * 4):
        p1 = pop1[random.randint(0, n - 1)]
        p2 = pop2[random.randint(0, n - 1)]
        games[(p1, p2)] = toolbox.fitness(p1, p2)
    leaders = [(k, v) for k, v in games.items()]
    leaders.sort(key = lambda item : item[1], reverse = True)
    l = int(math.log(n))
    leaders = leaders[:l]
    return leaders, leaders[0]


def change_hall(hall, part, size):
    if part[1] in [p[1] for p in hall]:
        return hall
    hall.append(deepcopy(part))
    hall.sort(key=lambda p: p[1], reverse=True)
    return hall[0:size]

def change_index(idx, chance, size):
    if size == 1:
        return idx
    if random.random() < chance:
        rnd = random.randint(0, size - 1)
        while rnd == idx:
            rnd = random.randint(0, size - 1)
        return rnd
    return idx

# /* Ordering
def init_rank_list(wf, nodes, estimator):
    wf_dag = HeftHelper.convert_to_parent_children_map(wf)
    return heftRank(wf_dag, nodes.get_nodes(), estimator)
    #return simpleRank(wf_dag)

def simpleRank(wf_dag):
    jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
    rank_list = dict()
    rank = 100
    for job in jobs:
        rank_list[job.id] = rank
        rank += 100
    return rank_list

def heftRank(wf, nodes, estimator):
    compcost = lambda job, agent: estimator.estimate_runtime(job, agent)
    commcost = lambda ni, nj, A, B: estimator.estimate_transfer_time(A, B, ni, nj)
    task_rank_cache = dict()
    return ranking_func(wf, nodes, compcost, commcost, task_rank_cache)

def ranking_func(wf_dag, nodes, compcost, commcost, task_rank_cache):
    rank = partial(HeftHelper.ranking, nodes=nodes, succ=wf_dag,
                             compcost=compcost, commcost=commcost,
                             task_rank_cache=task_rank_cache)
    jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
    rank_list = dict()
    for job in jobs:
        rank_list[job.id] = rank(job)

    return rank_list
# Ordering */


