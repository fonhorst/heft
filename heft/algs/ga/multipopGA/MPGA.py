from copy import deepcopy
from functools import reduce
import functools
import operator

from deap import base, tools, creator
from deap.tools import migRing
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.ga.GAImplementation.GAImpl import GAFactory, SynchronizedCheckpointedGA
from heft.core.environment.ResourceManager import Schedule


def create_mpga(*args, **kwargs):
    ##==========================
    ## reading params and initializing
    ##==========================
    ga_params = kwargs["ga_params"]

    migrCount = kwargs["migrCount"]
    emigrant_selection = kwargs["emigrant_selection"]

    all_iters_count = kwargs["all_iters_count"]
    POPSIZE = ga_params["population"]
    GENERATIONS = ga_params["generations"]
    MERGED_POP = ga_params.get("merged_pop_iters", 0)
    MIGRATIONS = int((all_iters_count - MERGED_POP)/GENERATIONS)

    is_silent = kwargs["silent"]
    wf = kwargs["wf"]
    rm = kwargs["resource_manager"]
    estimator = kwargs["estimator"]

    ga_functions = GAFunctions2(wf, rm, estimator)
    kwargs["ga_functions"] = ga_functions

    alg_builder = kwargs.get("alg_builder", lambda : GAFactory.default().create_ga(**kwargs))

    ##==========================
    ## create ga_alg
    ##==========================
    ga_alg = alg_builder()

    class MPGAComputation(SynchronizedCheckpointedGA):

        def __init__(self):
            super().__init__()
            pass

        #@profile_decorator
        def __call__(self, fixed_schedule_part, initial_schedule, current_time=0, initial_population=None, only_new_pops=False):
            ##==========================
            ## create populations
            ##==========================
            ##TODO: remake it
            creator.create("FitnessMax", base.Fitness, weights=(1.0,))
            creator.create("Individual", dict, fitness=creator.FitnessMax)

            toolbox = base.Toolbox()
            toolbox.register("evaluate", ga_functions.build_fitness(fixed_schedule_part, current_time))
            toolbox.register("attr_bool", ga_functions.build_initial(fixed_schedule_part, current_time))
            # Structure initializers
            toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.attr_bool)
            toolbox.register("population", tools.initRepeat, list, toolbox.individual)

            ## TODO: replace it with strategy and specilized builder
            if only_new_pops is True:
                ## TODO: replace this magic number with parameter
                populations = [toolbox.population(n=POPSIZE) for i in range(3)]
            else:
                ## create populations
                old_pop = initial_population
                newpop = toolbox.population(n=POPSIZE)
                #heft_initial = GAFunctions2.schedule_to_chromosome(initial_schedule)
                # TODO: this is a stub. Rearchitect information flows and entities responsibilities as soon as you will get the first positive results.
                heft_initial = initial_schedule if not isinstance(initial_schedule, Schedule) else GAFunctions2.schedule_to_chromosome(initial_schedule, fixed_schedule_part)
                heft_initial = tools.initIterate(creator.Individual, lambda: heft_initial)
                heft_pop = [ga_functions.mutation(deepcopy(heft_initial)) for i in range(POPSIZE)]
                populations = [old_pop, newpop, heft_pop]

            ## TODO: replace this more effective implementation
            def quick_save():
                whole_pop = reduce(lambda x, y: x+y, populations)
                ## choose new old pop for the next run
                sorted_whole_pop = sorted(whole_pop, key=lambda x: x.fitness.values, reverse=True)
                best = sorted_whole_pop[0]
                new_oldpop = sorted_whole_pop[0:POPSIZE]

                ## construct final result
                ## TODO: implement constructor of final result here
                result = ((best, new_oldpop, None, None), common_logbook)
                self._save_result(result)
                return result



            ##==========================
            ## run for several migration periods
            ##==========================
            common_logbook = tools.Logbook()
            result = None
            for k in range(MIGRATIONS):
                new_pops = []
                iter_map = {}

                for pop in populations:
                    ((best, npop, schedule, stopped_iteration), logbook) = ga_alg(fixed_schedule_part,
                           None,
                           current_time=current_time,
                           initial_population=pop)
                    new_pops.append(npop)
                    # for rec in logbook:
                    #     iter = k*GENERATIONS + rec["iter"]
                    #     mp = iter_map.get(iter, [])
                    #     mp.append({"worst": rec["worst"], "best": rec["best"], "avr": rec["avr"]})
                    #     iter_map[iter] = mp
                    #     pass
                for iter, items in iter_map.items():
                    best = max(it["best"] for it in items)
                    avr = sum(it["avr"] for it in items)/len(items)
                    worst = min(it["worst"] for it in items)
                    common_logbook.record(iter=iter, worst=worst, best=best, avr=avr)
                    pass
                populations = new_pops
                migRing(populations, migrCount, emigrant_selection)
                result = quick_save()
                pass

            # merge all populations in one and evaluate for some time
            # TODO: test this changes
            common_pop = functools.reduce(operator.add, populations, [])
            for k in range(MERGED_POP):
                ((best, npop, schedule, stopped_iteration), logbook) = ga_alg(fixed_schedule_part,
                           None,
                           current_time=current_time,
                           initial_population=common_pop)
                common_pop = npop
                # for rec in logbook:
                #     iter = (all_iters_count - MERGED_POP) + rec["iter"]
                #     common_logbook.record(iter=iter, worst=rec["worst"], best=rec["best"], avr=rec["avr"])
                #     pass
                result = quick_save()
                pass

            ((best, new_oldpop, x1, x2), x3) = result
            result = ((best, new_oldpop, ga_functions.build_schedule(best, fixed_schedule_part, current_time), None), common_logbook)
            self._save_result(result[0])
            return result
        pass
    return MPGAComputation()




