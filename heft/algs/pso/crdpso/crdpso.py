from copy import deepcopy
import random
import math
from heft.algs.heft.HeftHelper import HeftHelper
from functools import partial
from heft.algs.common.utilities import gather_info

def run_cpso(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, initial_pop=None, **params):

    pop = initial_pop

    w, c1, c2 = params["w"], params["c1"], params["c2"]

    n = len(pop) if pop is not None else params["n"]

    best = params.get('best', None)

    hallOfFame = []
    hallOfFameSize = int(math.log(n))
    if hallOfFameSize == 0:
        raise ValueError("hall_of_fame_size = 0")

    bestIndex = 0
    changeChance = 0.1

    if pop is None:
        pop = toolbox.population(n)
    pop2 = toolbox.config_population(n)

    for g in range(gen_curr, gen_curr + gen_step, 1):
        #print("g: {0}".format(g))
        leaders, winner = gamble(pop, pop2, n, toolbox)

        p1_leader = leaders[random.randint(0, len(leaders)-1)][0][0]
        p2_leader = leaders[random.randint(0, len(leaders)-1)][0][1]
        # fitness for pop1
        for p in pop:
            if not hasattr(p, "fitness") or not p.fitness.valid:
                p.fitness = toolbox.fitness(p, p2_leader)
            if not p.best or p.best.fitness < p.fitness:
                p.best = deepcopy(p)

            if not best or hallOfFame[hallOfFameSize-1][1] < p.fitness:
                hallOfFame = changeHall(hallOfFame, ((p, p2_leader), p.fitness), hallOfFameSize)

        # fitness for pop2
        for p in pop2:
            if not hasattr(p, "fitness") or not p.fitness.valid:
                p.fitness = toolbox.fitness(p1_leader, p)
            if not p.best or p.best.fitness < p.fitness:
                p.best = deepcopy(p)

            if not best or hallOfFame[hallOfFameSize-1][1] < p.fitness:
                hallOfFame = changeHall(hallOfFame, ((p1_leader, p), p.fitness), hallOfFameSize)

        # is winner best?
        if hallOfFame[hallOfFameSize-1][1] < winner[1]:
            hallOfFame = changeHall(hallOfFame, winner, hallOfFameSize)

        # Gather all the fitnesses in one list and print the stats
        #gather_info(logbook, stats, g, pop)

        bestIndex = changeIndex(bestIndex, changeChance, hallOfFameSize)
        best = hallOfFame[bestIndex]

        # update pop1
        for p in pop:
            toolbox.update(w, c1, c2, p, best[0][0])
        if invalidate_fitness and not g == gen_step-1:
            for p in pop:
                del p.fitness

        # update pop2
        for p in pop2:
            toolbox.config_update(w, c1, c2, p, best[0][1])
        if invalidate_fitness and not g == gen_step-1:
            for p in pop2:
                del p.fitness

    hallOfFame.sort(key=lambda p: p[1], reverse=True)
    best = hallOfFame[0]

    return pop, logbook, best


def velocity_update(w, c1, c2, pbest, gbest, velocity, particle):
    r1 = random.random()
    r2 = random.random()

    old_velocity = velocity*w
    pbest_velocity = (pbest - particle)*(c1*r1)
    gbest_velocity = (gbest - particle)*(c2*r2)

    new_velocity = old_velocity + pbest_velocity + gbest_velocity
    return new_velocity

# /* Hall of fame
def changeHall(hall, part, size):
    if part[1] in [p[1] for p in hall]:
        return hall
    hall.append(deepcopy(part))
    hall.sort(key=lambda p: p[1], reverse=True)
    return hall[0:size]

def changeIndex(idx, chance, size):
    if random.random() < chance:
        rnd = int(random.random() * size)
        while (rnd == idx):
            rnd = int(random.random() * size)
        return rnd
    return idx
# Hall of fame */

def gamble(pop1, pop2, n, toolbox):
    games = {}
    for _ in range(n * 4):
        p1 = pop1[random.randint(0, n - 1)]
        p2 = pop2[random.randint(0, n - 1)]
        games[(p1, p2)] = toolbox.fitness(p1, p2)
    leaders = [(k, v) for k, v in games.items()]
    leaders.sort(key = lambda item : item[1], reverse = True)
    l = int(math.log(n))
    leaders = leaders[:l]
    return leaders, leaders[0]

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








