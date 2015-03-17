import copy
import functools
import operator
import random
from threading import Lock

from deap import tools
from deap import creator
from deap import base
import deap
from deap.tools import History
import numpy
from heft.algs.common.individuals import FitnessStd, DictBasedIndividual
from heft.algs.common.utilities import gather_info
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import timing


class IteratedCheckpointedAlg:

    def __init__(self, alg):

        self._alg = alg

        self.current_result = None
        self.lock = Lock()

        self._is_stopped = False
        self.stop_lock = Lock()
        self._current_pop = None
        pass

    def __call__(self, logbook, stats, initial_pop, gen, **kwargs):
        best = None

        for g in range(gen):
            new_pop, new_logbook, new_best = self._iteration(logbook, stats, g, initial_pop, best, **kwargs)
            initial_pop, logbook, best = new_pop, new_logbook, new_best
            self._save_result((initial_pop, logbook, best))
            self._save_pop(initial_pop)
        pass


    def _iteration(self, logbook, stats, gen_curr, pop, best, **params):
        pop, logbook, new_best = self._alg(logbook=logbook, stats=stats, gen_curr=gen_curr, initial_pop=pop, best=best, invalidate_fitness=True, **params)
        return pop, logbook, new_best


    ## need to implement
    def _construct_result(self, result):
        return result

    def _save_result(self, result):
        self.lock.acquire()
        self.current_result = result
        self.lock.release()
        pass

    def _get_result(self):
        self.lock.acquire()
        result = self.current_result
        self.lock.release()
        return result

    def _save_pop(self, pop):
        self.lock.acquire()
        self._current_pop = copy.deepcopy(pop)
        self.lock.release()
        pass

    def get_result(self):
        self.lock.acquire()
        result = self.current_result
        self.lock.release()
        constructed = self._construct_result(result)
        return constructed

    def get_pop(self):
        self.lock.acquire()
        result = self._current_pop
        self.lock.release()
        return result

    def stop(self):
        self.stop_lock.acquire()
        self._is_stopped = True
        self.stop_lock.release()

    def is_stopped(self):
        self.stop_lock.acquire()
        result = self._is_stopped
        self.stop_lock.release()
        return result


def partially_fixed_schedule():
    raise NotImplementedError()


def run_ga(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, initial_pop=None, **params):
    """
    toolbox must have the methods:
    generate
    evaluate
    clone
    mate
    sweep_mutation
    mutate
    select_parents
    select
    """
    N = len(initial_pop) if initial_pop is not None else params["n"]
    pop = initial_pop if initial_pop is not None else toolbox.generate(N)
    CXPB, SWEEPMUTPB, MUTPB, KBEST = params["cxpb"], params["sweepmutpb"], params["mutpb"], params["kbest"]
    IS_SILENT = params["is_silent"]

    hallOfFame = deap.tools.HallOfFame(KBEST)


    # Evaluate the entire population


    ## This should map operator taken from toolbox to provide facilities for future parallelization
    ## EXAMPLE:
    ## fitnesses = list(map(toolbox.evaluate, pop))
    ## for ind, fit in zip(pop, fitnesses):
    ##    ind.fitness.values = fit
    for p in pop:
        p.fitness = toolbox.evaluate(p)

    best = None

    for g in range(gen_curr, gen_step, 1):
        # print("Iteration")

        hallOfFame.update(pop)

        # Select the next generation individuals

        parents = list(map(toolbox.clone, pop))
        # select_parents must return list of pairs [(par1,par2),]
        offsprings = toolbox.select_parents(parents) if hasattr(toolbox, "select_parents") else list(zip(parents[::2], parents[1::2]))
         # Clone the selected individuals

        # Apply crossover and mutation on the offspring
        for child1, child2 in offsprings:
            if random.random() < CXPB:
                toolbox.mate(child1, child2)
                del child1.fitness.values
                del child2.fitness.values
        offsprings = functools.reduce(operator.add, [[child1, child2] for child1, child2 in offsprings], [])
        for mutant in offsprings:
            if random.random() < SWEEPMUTPB:
                toolbox.sweep_mutation(mutant)
                del mutant.fitness.values
                continue
            if random.random() < MUTPB:
                toolbox.mutate(mutant)
                del mutant.fitness.values


        # Evaluate the individuals with an invalid fitness
        invalid_ind = [ind for ind in offsprings if not ind.fitness.valid]

        ## This should map operator taken from toolbox to provide facilities for future parallelization
        for p in invalid_ind:
            p.fitness = toolbox.evaluate(p)

        # mix with the best individuals of the time
        sorted_pop = sorted(pop + list(hallOfFame) + list(offsprings), key=lambda x: x.fitness, reverse=True)
        pop = sorted_pop[:KBEST:] + toolbox.select(sorted_pop[KBEST:], N - KBEST)

        gather_info(logbook, stats, g, pop, None, need_to_print=not IS_SILENT)

        best = max(pop, key=lambda x: x.fitness)
        pass

    return pop, logbook, best


def generate(n, ga_functions, fixed_schedule_part,
             current_time, init_sched_percent,
             initial_schedule):
    init_ind_count = int(n*init_sched_percent)
    res = []
    if initial_schedule is not None and init_ind_count > 0:
        ga_functions.initial_chromosome = DictBasedIndividual(GAFunctions2.schedule_to_chromosome(initial_schedule))
        init_chromosome = ga_functions.initial_chromosome
        init_arr = [copy.deepcopy(init_chromosome) for _ in range(init_ind_count)]
        res = res + init_arr
    if n - init_ind_count > 0:
        generated_arr = [DictBasedIndividual(ga_functions.random_chromo(fixed_schedule_part, current_time))
                         for _ in range(n - init_ind_count)]
        res = res + generated_arr
    return res


def fit_converter(func):
    def wrap(*args, **kwargs):
        x = func(*args, **kwargs)
        return FitnessStd(values=(1/x[0], 0.0))
    return wrap
