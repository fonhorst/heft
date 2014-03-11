import cProfile
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import copy
import datetime

import pstats
import random
from threading import Thread, Lock
import io
import threading
import math
from GA.DEAPGA.GAImplementation.GAFunctions2 import GAFunctions2, mark_finished
from GA.DEAPGA.GAImplementation.TemporaryEvaluate import create_builder, temp_fitness, temp_fit_for_all
from GA.DEAPGA.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from core.DSimpleHeft import DynamicHeft
from core.HeftHelper import HeftHelper
from core.comparisons.ComparisonBase import ComparisonUtility
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from core.simple_heft import StaticHeftPlanner
from deap import tools
from deap import creator
from deap import base
from environment.Resource import ResourceGenerator
from environment.ResourceManager import Schedule, ScheduleItem
from environment.Utility import Utility
import multiprocessing
from multiprocessing import Pool


Params = namedtuple('Params', ['ideal_flops',
                               'population',
                               'crossover_probability',
                               'replacing_mutation_probability',
                               'sweep_mutation_probability',
                               'generations'])
def default_fixed_schedule_part(resource_manager):
    fix_schedule_part = Schedule({node: [] for node in HeftHelper.to_nodes(resource_manager.get_resources())})
    return fix_schedule_part

def construct_ga_alg(is_silent, wf, resource_manager, estimator, params=Params(20, 300, 0.8, 0.5, 0.4, 50), context=None):

    population = params.population
    nodes = list(HeftHelper.to_nodes(resource_manager.get_resources()))

    def compcost(job, agent):
        return estimator.estimate_runtime(job, agent)

    def commcost(ni, nj, A, B):
        return estimator.estimate_transfer_time(A, B, ni, nj)

    ranking = HeftHelper.build_ranking_func(nodes, compcost, commcost)
    sorted_tasks = ranking(wf)

    #ga_functions = GAFunctions(wf, nodes, sorted_tasks, estimator, population)
    ga_functions = GAFunctions2(wf, nodes, sorted_tasks, resource_manager, estimator, population)


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
    fix_schedule_part = default_fixed_schedule_part(resource_manager)
    toolbox.register("evaluate", ga_functions.build_fitness(fix_schedule_part, 0))
    # toolbox.register("mate", tools.cxOnePoint)
    # toolbox.register("mate", tools.cxTwoPoints)
    # toolbox.register("mate", tools.cxUniform, indpb=0.2)

    toolbox.register("mate", ga_functions.crossover)
    toolbox.register("mutate", ga_functions.mutation)
    toolbox.register("select", tools.selTournament, tournsize=10)
    # toolbox.register("select", tools.selRoulette)

    repeated_best_count = 10
    ## TODO: add here
    ## TODO:  - ability to use external pop as initial pop
    ## TODO:    (in some circumstances it's very hard to do because of ga is relatively slow
    ## TODO:    and cannot quickly construct its solution,
    ## TODO:    Also we cannot just use the old ga population in case of extending interval of planning,
    ## TODO:    due to we must take into account new part of schedule generated by Heft)
    ## TODO:    Implementation of it should be delayed for more analysis in this field
    ## TODO:  - ability to include for fitness fixed part of schedule
    ## TODO:     (genome's construction stays the same, but we need to account fixed part of schedule)
    ## TODO:  - ability to save results per generation turn

    ## TODO: perhaps it can be represent as a dead point of Y-combinator

    ## TODO: redesign it as a stoppable ga
    class GAComputation:

        EVOLUTION_STOPPED_ITERATION_NUMBER = "EvoStpdIterNum"

        def __init__(self):
            self.current_result = None
            self.lock = Lock()

            self._is_stopped = False
            self.stop_lock = Lock()
            self._current_pop = None
            pass

        def __call__(self, fixed_schedule_part, initial_schedule, current_time=0, initial_population=None):
            print("Evaluating...")
            toolbox.register("evaluate", ga_functions.build_fitness(fixed_schedule_part, current_time))
            toolbox.register("attr_bool", ga_functions.build_initial(fixed_schedule_part, current_time))
            # Structure initializers
            toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.attr_bool)
            toolbox.register("population", tools.initRepeat, list, toolbox.individual)

            ga_functions.initial_chromosome = GAFunctions2.schedule_to_chromosome(initial_schedule)
            CXPB, MUTPB, NGEN = params.crossover_probability, params.replacing_mutation_probability, params.generations
            SWEEPMUTPB = params.sweep_mutation_probability

            if initial_population is None:
                initial_population = []
            pop = initial_population + toolbox.population(n=population - len(initial_population))
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

                # check if evolution process has stopped
                if len(previous_raised_avr_individuals) == repeated_best_count:
                    length = len(previous_raised_avr_individuals)
                    whole_sum = sum(previous_raised_avr_individuals)
                    mean = whole_sum / length
                    sum2 = sum(abs(x - mean) for x in previous_raised_avr_individuals)
                    std = sum2/length
                    ## TODO: uncomment it later. output
                    # print("std: " + str(std))
                    if std < 0.0001:
                        print(" Evolution process has stopped at " + str(g) + " iteration")
                        res = self._get_result()
                        extended_result = (res[0], res[1], res[2], res[3], g)
                        self._save_result(extended_result)
                        break


                # print("-- Generation %i --" % g)
                # Select the next generation individuals
                offspring = toolbox.select(pop, len(pop))
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
                    if random.random() < MUTPB:
                        toolbox.mutate(mutant)
                        del mutant.fitness.values

                # Evaluate the individuals with an invalid fitness
                invalid_ind = [ind for ind in offspring if not ind.fitness.valid]

                fitnesses = list(map(toolbox.evaluate, invalid_ind))

                for ind, fit in zip(invalid_ind, fitnesses):
                    ind.fitness.values = fit
                pop[:] = offspring
                # Gather all the fitnesses in one list and print the stats
                fits = [ind.fitness.values[0] for ind in pop]

                length = len(pop)
                mean = sum(fits) / length
                sum2 = sum(x*x for x in fits)
                std = abs(sum2 / length - mean**2)**0.5

                if not is_silent:
                    print("-- Generation %i --" % g)
                    print("  Worst %s" % str(1/min(fits)))
                    print("   Best %s" % str(1/max(fits)))
                    print("    Avg %s" % str(1/mean))
                    print("    Std %s" % str(1/std))

                best = self._find_best(pop)
                # the last component is iteration number when evolution stopped
                result = (best, pop, fixed_schedule_part, current_time, g)
                self._save_result(result)
                self._save_pop(pop)

                if len(previous_raised_avr_individuals) == repeated_best_count:
                    previous_raised_avr_individuals = previous_raised_avr_individuals[1::]
                previous_raised_avr_individuals.append(1/max(fits))

                pass

            # best = self._find_best(pop)
            # self._save_result((best, pop, fixed_schedule_part, current_time))

            ## return the best fitted individual and resulted population
            print("Ready")
            return self.get_result()

        def _find_best(self, pop):
            # resulted_pop = [(ind, ind.fitness.values[0]) for ind in pop]
            # result = max(resulted_pop, key=lambda x: x[1])
            # return result[0]
            result = max(pop, key=lambda x: x.fitness.values[0])
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

        def _construct_result(self, result):
            (best, pop, fixed_schedule_part, current_time, stopped_iteration) = result
            ## TODO: make additional structure to return elements
            return best, pop, ga_functions.build_schedule(best, fixed_schedule_part, current_time), stopped_iteration

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

        pass




    return GAComputation()


def build(wf_name, is_silent=False, is_visualized=True, params=Params(20, 400, 0.9, 0.7, 0.5, 150), nodes_conf = None):
    print("Proccessing " + str(wf_name))

    dax1 = '../../resources/' + wf_name + '.xml'

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000
    ideal_flops = params.ideal_flops
    population = params.population

    wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
    rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4,
                                 min_flops=5,
                                 max_flops=10)
    resources = rgen.generate()
    transferMx = rgen.generateTransferMatrix(resources)

    if nodes_conf is None:
        ##TODO: remove it later
        dax2 = '../../resources/' + 'CyberShake_30' + '.xml'
        path = '../../resources/saved_schedules/' + 'CyberShake_30_bundle_backup' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1))
        resources = bundle.dedicated_resources
        #transferMx = bundle.transfer_mx
        ideal_flops = bundle.ideal_flops
        ##TODO: end
    else:
        ## TODO: refactor it later.
        resources = ResourceGenerator.r(nodes_conf)
        ##


    estimator = ExperimentEstimator(transferMx, ideal_flops, dict())
    resource_manager = ExperimentResourceManager(resources)
    alg_func = construct_ga_alg(is_silent, wf, resource_manager, estimator, params)


        ##TODO: remove it later
       ##print("======END-END======")
        ##return None
    ## TODO: initial schedule must be the second part of fixed_schedule
    def main(initial_schedule):
        fix_schedule_part = default_fixed_schedule_part(resource_manager)
        (the_best_individual, pop, schedule) = alg_func(fix_schedule_part, initial_schedule)

        max_makespan = Utility.get_the_last_time(schedule)
        seq_time_validaty = Utility.validateNodesSeq(schedule)
        mark_finished(schedule)
        dependency_validaty = Utility.validateParentsAndChildren(schedule, wf)
        transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(schedule_dynamic_heft, wf, estimator)
        print("=============Results====================")
        print("              Makespan %s" % str(max_makespan))
        print("          Seq validaty %s" % str(seq_time_validaty))
        print("   Dependancy validaty %s" % str(dependency_validaty))
        print("    Transfer validaty %s" % str(transfer_dependency_validaty))

        name = wf_name +"_bundle"
        path = '../../resources/saved_schedules/' + name + '.json'
        Utility.save_schedule(path, wf_name, resources, transferMx, ideal_flops, schedule)

        if is_visualized:
            Utility.create_jedule_visualization(schedule, wf_name+'_ga')
        pass


    ##================================
    ##Dynamic Heft Run
    ##================================
    dynamic_planner = DynamicHeft(wf, resource_manager, estimator)

    nodes = HeftHelper.to_nodes(resource_manager.resources)
    current_cleaned_schedule = Schedule({node: [] for node in nodes})
    schedule_dynamic_heft = dynamic_planner.run(current_cleaned_schedule)
    dynamic_heft_makespan = Utility.get_the_last_time(schedule_dynamic_heft)
    dynamic_seq_time_validaty = Utility.validateNodesSeq(schedule_dynamic_heft)
    mark_finished(schedule_dynamic_heft)
    dynamic_dependency_validaty = Utility.validateParentsAndChildren(schedule_dynamic_heft, wf)
    transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(schedule_dynamic_heft, wf, estimator)
    print("heft_makespan: " + str(dynamic_heft_makespan))
    print("=============Dynamic HEFT Results====================")
    print("              Makespan %s" % str(dynamic_heft_makespan))
    print("          Seq validaty %s" % str(dynamic_seq_time_validaty))
    print("   Dependancy validaty %s" % str(dynamic_dependency_validaty))
    print("    Transfer validaty %s" % str(transfer_dependency_validaty))

    if is_visualized:
        Utility.create_jedule_visualization(schedule_dynamic_heft, wf_name+'_heft')


    ##================================
    ##GA Run
    ##================================
    # pr = cProfile.Profile()
    # pr.enable()
    main(schedule_dynamic_heft)
    # pr.disable()
    # s = io.StringIO()
    # sortby = 'cumulative'
    # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats()
    # print(s.getvalue())

