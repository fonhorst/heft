import copy
import random
from threading import Lock

from deap import tools
from deap import creator
from deap import base
import deap
from deap.tools import History
import numpy
from heft.algs.common.utilities import gather_info
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import timing


class SynchronizedCheckpointedGA:

    def __init__(self):
        self.current_result = None
        self.lock = Lock()

        self._is_stopped = False
        self.stop_lock = Lock()
        self._current_pop = None
        pass

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


class GAFactory:

    DEFAULT_IDEAL_FLOPS = 20
    DEFAULT_POPULATION = 50
    DEFAULT_CROSSOVER_PROBABILITY = 0.8
    DEFAULT_REPLACING_MUTATION_PROBABILITY = 0.5
    DEFAULT_SWEEP_MUTATION_PROBABILITY = 0.4
    DEFAULT_GENERATIONS = 100

    _default_instance = None

    @staticmethod
    def default():
        if GAFactory._default_instance is None:
            GAFactory._default_instance = GAFactory()
        return GAFactory._default_instance

    def __init__(self):
        pass

    def create_ga(self, *args, **kwargs):
        is_silent = kwargs["silent"]
        wf = kwargs["wf"]
        rm = kwargs["resource_manager"]
        estimator = kwargs["estimator"]
        ga_params = kwargs["ga_params"]


        POPSIZE = ga_params.get("population", self.DEFAULT_POPULATION)
        CXPB = ga_params.get('crossover_probability', self.DEFAULT_CROSSOVER_PROBABILITY)
        MUTPB = ga_params.get('replacing_mutation_probability', self.DEFAULT_REPLACING_MUTATION_PROBABILITY)
        NGEN = ga_params.get('generations', self.DEFAULT_GENERATIONS)
        SWEEPMUTPB = ga_params.get('sweep_mutation_probability', self.DEFAULT_SWEEP_MUTATION_PROBABILITY)

        Kbest = ga_params.get('Kbest', POPSIZE)


        ga_functions = kwargs.get("ga_functions", GAFunctions2(wf, rm, estimator))

        check_evolution_for_stopping = kwargs.get("check_evolution_for_stopping", True)


        def default_fixed_schedule_part(resource_manager):
            fix_schedule_part = Schedule({node: [] for node in HeftHelper.to_nodes(resource_manager.get_resources())})
            return fix_schedule_part



        ##================================
        ##Create genetic algorithm here
        ##================================
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", dict, fitness=creator.FitnessMax)

        toolbox = base.Toolbox()
        # Attribute generator
        toolbox.register("attr_bool", ga_functions.build_initial(None, 0))
        # Structure initializers
        toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.attr_bool)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)


        ## default case
        fix_schedule_part = default_fixed_schedule_part(rm)
        toolbox.register("evaluate", ga_functions.build_fitness(fix_schedule_part, 0))


        toolbox.register("mate", ga_functions.crossover)
        toolbox.register("mutate", ga_functions.mutation)
        # toolbox.register("select", tools.selTournament, tournsize=4)
        toolbox.register("select", tools.selRoulette)
        # toolbox.register("select", tools.selBest)
        # toolbox.register("select", tools.selTournamentDCD)
        # toolbox.register("select", tools.selNSGA2)

        repeated_best_count = 10

        class GAComputation(SynchronizedCheckpointedGA):

            EVOLUTION_STOPPED_ITERATION_NUMBER = "EvoStpdIterNum"

            def __init__(self):
                super().__init__()
                pass

            @timing
            def __call__(self, fixed_schedule_part, initial_schedule, current_time=0, initial_population=None):
                print("Evaluating...")
                toolbox.register("evaluate", ga_functions.build_fitness(fixed_schedule_part, current_time))
                toolbox.register("attr_bool", ga_functions.build_initial(fixed_schedule_part, current_time))
                # Structure initializers
                toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.attr_bool)
                toolbox.register("population", tools.initRepeat, list, toolbox.individual)


                ga_functions.initial_chromosome = GAFunctions2.schedule_to_chromosome(initial_schedule)


                if initial_population is None:
                    initial_population = []
                pop = initial_population + toolbox.population(n=POPSIZE - len(initial_population))

                ## TODO: experimental change
                history = History()

                # Decorate the variation operators
                #toolbox.decorate("mate", history.decorator)
                # toolbox.decorate("mutate", history.decorator)

                # Create the population and populate the history
                #history.update(pop)
                #===================================================

                hallOfFame = deap.tools.HallOfFame(5)

                stats = tools.Statistics(key=lambda x: 1/x.fitness.values[0])
                stats.register("min", numpy.min)
                stats.register("max", numpy.max)
                stats.register("avr", numpy.mean)
                stats.register("std", numpy.std)

                logbook = tools.Logbook()
                logbook.header = ["gen"] + stats.fields


                # Evaluate the entire population
                fitnesses = list(map(toolbox.evaluate, pop))
                for ind, fit in zip(pop, fitnesses):
                    ind.fitness.values = fit



                previous_raised_avr_individuals = []

                # Begin the evolution
                for g in range(NGEN):
                    # print("Iteration")
                    if self.is_stopped():
                        break

                    hallOfFame.update(pop)

                    # logbook.record(pop=copy.deepcopy(pop))

                    # check if evolution process has stopped

                    # if (check_evolution_for_stopping is True) and len(previous_raised_avr_individuals) == repeated_best_count:
                    #     length = len(previous_raised_avr_individuals)
                    #     whole_sum = sum(previous_raised_avr_individuals)
                    #     mean = whole_sum / length
                    #     sum2 = sum(abs(x - mean) for x in previous_raised_avr_individuals)
                    #     std = sum2/length
                    #     ## TODO: uncomment it later. output
                    #     # print("std: " + str(std))
                    #     if std < 0.0001:
                    #         print(" Evolution process has stopped at " + str(g) + " iteration")
                    #         res = self._get_result()
                    #         extended_result = (res[0], res[1], res[2], res[3], g)
                    #         self._save_result(extended_result)
                    #         break





                    # print("-- Generation %i --" % g)
                    # Select the next generation individuals
                    offspring = pop#toolbox.select(pop, len(pop))
                    # Clone the selected individuals
                    offspring = list(map(toolbox.clone, offspring))
                    # Apply crossover and mutation on the offspring
                    for child1, child2 in zip(offspring[::2], offspring[1::2]):
                        if random.random() < CXPB:
                            toolbox.mate(child1, child2)
                            del child1.fitness.values
                            del child2.fitness.values

                    for mutant in offspring:
                        if random.random() < SWEEPMUTPB:
                            ga_functions.sweep_mutation(mutant)
                            del mutant.fitness.values
                            continue
                        if random.random() < MUTPB:
                            toolbox.mutate(mutant)
                            del mutant.fitness.values


                    # Evaluate the individuals with an invalid fitness
                    invalid_ind = [ind for ind in offspring if not ind.fitness.valid]

                    fitnesses = list(map(toolbox.evaluate, invalid_ind))

                    for ind, fit in zip(invalid_ind, fitnesses):
                        ind.fitness.values = fit
                    #pop[:] = offspring

                    # mix with the best individuals of the time
                    sorted_pop = sorted(pop + list(hallOfFame) + offspring, key=lambda x: x.fitness.values, reverse=True)
                    pop = sorted_pop[:Kbest:] + toolbox.select(sorted_pop[Kbest:], POPSIZE - Kbest)



                    # Gather all the fitnesses in one list and print the stats
                    fits = [ind.fitness.values[0] for ind in pop]

                    length = len(pop)
                    mean = sum(fits) / length
                    sum2 = sum(x*x for x in fits)
                    std = abs(sum2 / length - mean**2)**0.5
                    worst = 1/min(fits)
                    best = 1/max(fits)
                    avr = 1/mean

                    data = stats.compile(pop)
                    logbook.record(gen=g, **data)
                    if not is_silent:
                        print(logbook.stream)
                        # print("-- Generation %i --" % g)
                        # print("  Worst %s" % str(worst))
                        # print("   Best %s" % str(best))
                        # print("    Avg %s" % str(avr))
                        # print("    Std %s" % str(1/std))

                    best = self._find_best(pop)
                    # the last component is iteration number when evolution stopped
                    result = (best, pop, fixed_schedule_part, current_time, g)
                    self._save_result(result)
                    self._save_pop(pop)

                    if len(previous_raised_avr_individuals) == repeated_best_count:
                        previous_raised_avr_individuals = previous_raised_avr_individuals[1::]
                    previous_raised_avr_individuals.append(1/mean)


                    pass
                #
                # import matplotlib.pyplot as plt
                # import networkx
                #
                # graph = networkx.DiGraph(history.genealogy_tree)
                # graph = graph.reverse()     # Make the grah top-down
                # colors = [toolbox.evaluate(history.genealogy_history[i])[0] for i in graph]
                # networkx.draw(graph, node_color=colors)
                # plt.show()


                # best = self._find_best(pop)
                # self._save_result((best, pop, fixed_schedule_part, current_time))

                ## return the best fitted individual and resulted population


                print("Ready")
                return self.get_result(), logbook

            def _find_best(self, pop):
                # resulted_pop = [(ind, ind.fitness.values[0]) for ind in pop]
                # result = max(resulted_pop, key=lambda x: x[1])
                # return result[0]
                result = max(pop, key=lambda x: x.fitness.values[0])
                return result

            def _construct_result(self, result):
                (best, pop, fixed_schedule_part, current_time, stopped_iteration) = result
                ## TODO: make additional structure to return elements
                return best, pop, ga_functions.build_schedule(best, fixed_schedule_part, current_time), stopped_iteration
            pass

        return GAComputation()
    pass


def run_ga(toolbox, logbook, stats, gen_curr, gen_step=1, invalidate_fitness=True, initial_pop=None, **params):

    N = len(initial_pop) if initial_pop is not None else params["n"]
    pop = initial_pop if initial_pop is not None else toolbox.generate(N)
    CXPB, SWEEPMUTPB, MUTPB, KBEST = params["cxpb"], params["sweepmutpb"], params["mutpb"], params["kbest"]
    IS_SILENT = params["is_silent"]

    hallOfFame = deap.tools.HallOfFame(5)


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
        if self.is_stopped():
            break

        hallOfFame.update(pop)

        # Select the next generation individuals

        # parents = toolbox.select_parents(pop)
        parents = zip(pop[::2], pop[1::2])
         # Clone the selected individuals
        offsprings = list(map(toolbox.clone, parents))
        # Apply crossover and mutation on the offspring
        for child1, child2 in offsprings:
            if random.random() < CXPB:
                toolbox.mate(child1, child2)
                del child1.fitness.values
                del child2.fitness.values

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
        sorted_pop = sorted(pop + list(hallOfFame) + offsprings, key=lambda x: x.fitness.values, reverse=True)
        pop = sorted_pop[:KBEST:] + toolbox.select(sorted_pop[KBEST:], N - KBEST)

        gather_info(logbook, stats, g, pop, need_to_print=IS_SILENT)

        best = max(pop, lambda x: x.fitness)

        # the last component is iteration number when evolution stopped
        result = (best, pop, fixed_schedule_part, current_time, g)
        self._save_result(result)
        self._save_pop(pop)

        pass

    return pop, logbook, best
