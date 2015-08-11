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
from copy import deepcopy
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


def run_hs(toolbox, logbook, stats, **params):
    N = params["n"]
    pop = toolbox.generate(N)
    IS_SILENT = params["is_silent"]
    gen_step = params["gen_step"]

    for p in pop:
        p.fitness = toolbox.evaluate(p)

    best = None

    for g in range(gen_step):
        r_example = toolbox.generate(1)[0]
        p_example = deepcopy(pop[random.randint(0, len(pop) - 1)])

        strat = random.random()
        if strat <= 0.33:
            example = r_example
        if 0.33 < strat <= 0.66:
            example = p_example
        if strat > 0.66:
            example = toolbox.mate(r_example, p_example)[0]

        fitness_pop = [p.fitness for p in pop]
        min_fit = min(fitness_pop)
        max_fit = max(fitness_pop)

        for imp in range(5):
            strat = random.random()
            if strat <= 0.5:
                example = toolbox.sweep_mutation(deepcopy(example))
            else:
                example = toolbox.mutate(deepcopy(example))
            example.fitness = toolbox.evaluate(example)
            if example.fitness > max_fit:
                break

        need_update = False
        if example.fitness > min_fit and example.fitness not in fitness_pop:
            pop.append(example)
            need_update = True
        if need_update:
            sorted_pop = sorted(pop, key=lambda x: x.fitness, reverse=True)
            pop = sorted_pop[:N]

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
