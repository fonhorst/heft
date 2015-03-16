"""
This is a prototype of set-based PSO algorithm directed to solve workflow scheduling problem with fixed ordering.
It must be refactored and made in reusable, configurable form later.

Position =>{task_name: node_name}
Velocity => {(task_name, node_name): probability}
"""
from copy import deepcopy
from functools import partial
import random
import heft.algs.pso.rdpso.ordering_operators

from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.individuals import FitAdapter
#from heft.algs.pso.rdpso.mapordschedule import fitness as basefitness
#from heft.algs.pso.rdpso.mapordschedule import MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.common.utilities import gather_info
from heft.core.environment.Utility import timing, RepeatableTiming
import heft.algs.pso.rdpso.ordering_operators
from heft.algs.heft.HeftHelper import HeftHelper
import math


#@RepeatableTiming(100)
def run_pso(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, initial_pop=None, **params):

    #print("PSO_STARTED")

    pop = initial_pop

    w, c1, c2 = params["w"], params["c1"], params["c2"]
    n = len(pop) if pop is not None else params["n"]
    #rm = params["rm"]
    #nodes = rm.get_nodes()
    wf = params["wf"]

    mapMatrix = params["mapMatrix"]
    rankList = params["rankList"]
    filterList = params["ordFilter"]
    #estimator = params["estimator"]

    best = params.get('best', None)

    hallOfFame = []
    hallOfFameSize = int(math.log(n))

    bestIndex = 0
    changeChance = 0.1

    #curFile = open("C:\Melnik\Experiments\Work\PSO_compare" + "\\"  +  "RDwithMap generations.txt", 'w')

    if pop is None:
        pop = toolbox.population(n)

    for g in range(gen_curr, gen_curr + gen_step, 1):
        #print("g: {0}".format(g))
        for p in pop:
            #if not heft.algs.pso.rdpso.ordering_operators.test_valid(wf, p.ordering, rankList, filterList, fixed_tasks_ids=[]):
            #    print("part " + str(pop.index(p)) + " incorrect before correct fitness update")
            if not hasattr(p, "fitness") or not p.fitness.valid:
            #if 1==1:
                p.fitness = toolbox.fitness(p, mapMatrix, rankList, filterList)
            if not p.best or p.best.fitness < p.fitness:
                p.best = deepcopy(p)
            if not best or hallOfFame[hallOfFameSize-1].fitness < p.fitness:
                hallOfFame = changeHall(hallOfFame, p, hallOfFameSize)
        # Gather all the fitnesses in one list and print the stats
        #gather_info(logbook, stats, g, pop)


        bestIndex = changeIndex(bestIndex, changeChance, hallOfFameSize)
        best = hallOfFame[bestIndex]

        """
        hallString = [part.fitness.values[0] for part in hallOfFame]
        curFile.write(str(g) + "   " + str(bestIndex) + "    " + str(hallString) +  "\n")
        particlesFitness = sorted([part.fitness.values[0] for part in pop])
        curFile.write(str(particlesFitness) + "\n")
        curFile.write("\n")
        """


        for p in pop:

            #if not heft.algs.pso.rdpso.ordering_operators.test_valid(wf, p.ordering, rankList, filterList, fixed_tasks_ids=[]):
            #    print("part " +  str(pop.index(p)) + " incorrect before update")
            #toolbox.update(w, c1, c2, p, best, pop, g)
            toolbox.update(w, c1, c2, p, best, pop)

            #if not heft.algs.pso.rdpso.ordering_operators.test_valid(wf, p.ordering, rankList, filterList, fixed_tasks_ids=[]):
            #    print("part " +  str(pop.index(p)) + " incorrect after update")
            #toolbox.update((w-w/gen_step*g), c1, c2, p, best, pop)
        if invalidate_fitness and not g == gen_step-1:
            for p in pop:
                del p.fitness
        #for p in pop:
            #if not heft.algs.pso.rdpso.ordering_operators.test_valid(wf, p.ordering, rankList, filterList, fixed_tasks_ids=[]):
            #    print("part " +  str(pop.index(p)) + " incorrect after delete fitness")
        pass


    """
    hallString = [part.fitness.values[0] for part in hallOfFame]
    curFile.write(str("FINAL") + "   " + str(bestIndex) + "    " + str(hallString) +  "\n")
    particlesFitness = sorted([part.fitness.values[0] for part in pop])
    curFile.write(str(particlesFitness) + "\n")
    curFile.write("\n")
    curFile.close()
    """


    hallOfFame.sort(key=lambda p:p.fitness, reverse=True)
    best = hallOfFame[0]

    return pop, logbook, best


def velocity_update(w, c1, c2, pbest, gbest, velocity, particle, pop):
    r1 = random.random()
    r2 = random.random()

    old_velocity = velocity*w
    pbest_velocity = (pbest - particle)*(c2*r2)
    gbest_velocity = (gbest - particle)*(c1*r1)

    new_velocity = old_velocity + pbest_velocity + gbest_velocity
    return new_velocity



def initMapMatrix(jobs, nodes, estimator):
    matrix = dict()
    for job in jobs:
        for node in nodes:
            matrix[job.id, node.name] = 100 / estimator.estimate_runtime(job, node)
    return matrix

def initRankList(wf_dag, nodes, estimator):
    return heftRank(wf_dag, nodes, estimator)


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

def mappingTransform(mapping, mapMatrix, nodes):
    res = dict()
    for k,v in mapping.items():
        subList =[(node, abs(mapMatrix[(k, node.name)] - v)) for node in nodes]
        appNode = (min(subList, key = lambda n : n[1]))[0]
        res[k] = appNode.name
    return res

def orderingTransform(ordering, rankList, filterList):
    res = []
    rankCopy = rankList.copy()
    for it in range(len(ordering)):
        val = ordering[it]
        curRankList = set()
        curFilter = filterList[val[0]]

        for item in rankCopy.items():
            #if item[0] in curFilter:
                curRankList.add(item)

        subList = [(task, abs(val[1] - rank)) for (task, rank) in curRankList]
        if (len(subList) == 0):
            pass
            #print("PRIVET")
        curTask = min(subList, key=lambda t: t[1])[0]
        res.append(curTask)
        if curTask != val[0]:
            swapTasks(ordering, val[0], curTask)
        del rankCopy[curTask]
    return res

def filterList(wf):
    wf_dag = HeftHelper.convert_to_parent_children_map(wf)
    jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
    resultFilter = {job.id:{j.id for j in jobs} for job in jobs}
    headTask = wf.head_task
    ready = dict()
    readyParents = dict()
    for task in headTask.children:
        getTaskChildFilter(task, resultFilter, ready)
    for task in jobs:
        getTaskParentFilter(task, resultFilter, readyParents)
    return resultFilter

def getTaskChildFilter(task, filter, ready):
    res = {task.id}
    for child in task.children:
        if child in ready:
            res.update(ready[child])
        else:
            res.update(getTaskChildFilter(child, filter, ready))
    filter[task.id].difference_update(res)
    filter[task.id].add(task.id)
    ready[task.id] = res
    return res

def getTaskParentFilter(task, filter,  ready):
    res = {task.id}
    for parent in task.parents:
        if parent.is_head:
            continue
        if parent in ready:
            res.update(ready[parent])
        else:
            res.update(getTaskParentFilter(parent, filter, ready))
    filter[task.id].difference_update(res)
    filter[task.id].add(task.id)
    ready[task.id] = res
    return res

def swapTasks(ordering, t1, t2):
    fstList = [item[0] for item in ordering]
    t1Idx = fstList.index(t1)
    t2Idx = fstList.index(t2)
    ordering[t2Idx] = (t1, ordering[t2Idx][1])
    ordering[t1Idx] = (t2, ordering[t1Idx][1])

def changeHall(hall, part, size):
    if part.fitness in [p.fitness for p in hall]:
        return hall
    hall.append(deepcopy(part))
    hall.sort(key=lambda p: p.fitness, reverse=True)
    return hall[0:size]

def changeIndex(idx, chance, size):
    if random.random() < chance:
        rnd = int(random.random() * size)
        while (rnd == idx):
            rnd = int(random.random() * size)
        return rnd
    return idx
