from functools import partial
import os
from deap import tools
from deap.tools import Logbook
from gi.overrides import deprecated
import numpy
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import UniqueNameSaver
from heft.experiments.comparison_experiments.executors.MIGaHeftExecutor import MIGaHeftExecutor
from heft.experiments.comparison_experiments.executors.MPGaHeftOldPopExecutor import MPGaHeftOldPopExecutor
from heft.experiments.comparison_experiments.executors.GaHeftExecutor import GaHeftExecutor
from heft.experiments.comparison_experiments.executors.GaHeftOldPopExecutor import GaHeftOldPopExecutor
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.settings import TEMP_PATH


def do_gaheft_exp(alg_builder, wf_name, **params):
    print("EXPERIMENT RUN START===========================")
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    ga = alg_builder(_wf, rm, estimator,
                     params["init_sched_percent"],
                     log_book=None, stats=None,
                     alg_params=params["alg_params"])
    machine = GaHeftExecutor(heft_planner=dynamic_heft,
                             wf=_wf,
                             resource_manager=rm,
                             ga_builder=lambda: ga,
                             **params["executor_params"])

    machine.init()
    machine.run()
    resulted_schedule = machine.current_schedule

    Utility.validate_dynamic_schedule(_wf, resulted_schedule)

    data = {
        "wf_name": wf_name,
        "params": params,
        "result": {
            "makespan": Utility.makespan(resulted_schedule),
            ## TODO: this function should be remade to adapt under conditions of dynamic env
            #"overall_transfer_time": Utility.overall_transfer_time(resulted_schedule, _wf, estimator),
            "overall_execution_time": Utility.overall_execution_time(resulted_schedule),
            "overall_failed_tasks_count": Utility.overall_failed_tasks_count(resulted_schedule)
        }
    }
    print("EXPERIMENT RUN END=========================")
    return data


def do_inherited_pop_exp(alg_builder, chromosome_cleaner_builder, wf_name, **params):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    chromosome_cleaner = chromosome_cleaner_builder(_wf, rm, estimator)
    dynamic_heft = DynamicHeft(_wf, rm, estimator)

    logbook = Logbook()

    stats = tools.Statistics(lambda ind: ind.fitness.values[0])
    stats.register("avg", numpy.mean)
    stats.register("std", numpy.std)
    stats.register("min", numpy.min)
    stats.register("max", numpy.max)

    ga = alg_builder(_wf, rm, estimator,
                     params["init_sched_percent"],
                     log_book=logbook, stats=stats,
                     alg_params=params["alg_params"])

    machine = GaHeftOldPopExecutor(heft_planner=dynamic_heft,
                             wf=_wf,
                             resource_manager=rm,
                             estimator=estimator,
                             stat_saver=None,
                             ga_builder=lambda: ga,
                             chromosome_cleaner=chromosome_cleaner,
                             **params["executor_params"])

    machine.init()
    print("Executor start")
    machine.run()
    print("Executor stop")

    resulted_schedule = machine.current_schedule
    stat_data = machine.executor_stat_data

    Utility.validate_dynamic_schedule(_wf, resulted_schedule)

    data = {
        "wf_name": wf_name,
        "params": params,
        "result": {
            "random_init_logbook": stat_data["random_init_logbook"],
            "inherited_init_logbook": stat_data["inherited_init_logbook"],
            "makespan": Utility.makespan(resulted_schedule),
            ## TODO: this function should be remade to adapt under conditions of dynamic env
            #"overall_transfer_time": Utility.overall_transfer_time(resulted_schedule, _wf, estimator),
            "overall_execution_time": Utility.overall_execution_time(resulted_schedule),
            "overall_failed_tasks_count": Utility.overall_failed_tasks_count(resulted_schedule)
        }
    }

    ##TODO: hack. Remove it later
    path = os.path.join(TEMP_PATH, "gaheft_series")
    saver = UniqueNameSaver(path, params["experiment_name"])
    saver(data)

    return data


def do_triple_island_exp(alg_builder, chromosome_cleaner_builder, schedule_to_chromosome_converter_builder, wf_name, **params):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    chromosome_cleaner = chromosome_cleaner_builder(_wf, rm, estimator)
    dynamic_heft = DynamicHeft(_wf, rm, estimator)


    mpga = alg_builder(_wf, rm, estimator,
                          params["init_sched_percent"],
                          log_book=None, stats=None,
                          alg_params=params["alg_params"])



    machine = MIGaHeftExecutor(heft_planner=dynamic_heft,
                               wf=_wf,
                               resource_manager=rm,
                               estimator=estimator,
                               ga_builder=lambda: mpga,
                               chromosome_cleaner=chromosome_cleaner,
                               schedule_to_chromosome_converter=schedule_to_chromosome_converter_builder(wf, rm, estimator),
                               **params["executor_params"])

    machine.init()
    machine.run()
    resulted_schedule = machine.current_schedule
    stat_data = machine.executor_stat_data

    Utility.validate_dynamic_schedule(_wf, resulted_schedule)

    data = {
        "wf_name": wf_name,
        "params": params,
        "result": {
            "makespan": Utility.makespan(resulted_schedule),
            ## TODO: this function should be remade to adapt under conditions of dynamic env
            #"overall_transfer_time": Utility.overall_transfer_time(resulted_schedule, _wf, estimator),
            "overall_execution_time": Utility.overall_execution_time(resulted_schedule),
            "overall_failed_tasks_count": Utility.overall_failed_tasks_count(resulted_schedule)
        }
    }

    ##TODO: hack. Remove it later
    path = os.path.join(TEMP_PATH, "migaheft_series")
    saver = UniqueNameSaver(path, params["experiment_name"])
    saver(data)

    return data

## TODO: deprecated
def do_island_inherited_pop_exp(alg_builder, mp_alg_builder, algorithm_builder, chromosome_cleaner_builder, schedule_to_chromosome_converter_builder, wf_name, **params):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    chromosome_cleaner = chromosome_cleaner_builder(_wf, rm, estimator)
    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    ga = alg_builder(_wf, rm, estimator,
                     params["init_sched_percent"],
                     log_book=None, stats=None,
                     alg_params=params["alg_params"])

    ## TODO: remake this part later.
    # def reverse_interface_adapter(func):
    #     def wrap(*args, **kwargs):
    #         (best, pop, resulted_schedule, _), logbook = func(*args, **kwargs)
    #         return pop, logbook, best
    #     return wrap


    mpga = mp_alg_builder(_wf, rm, estimator,
                          params["init_sched_percent"],
                          algorithm= partial(algorithm_builder, **params["alg_params"]),
                          #algorithm=reverse_interface_adapter(ga),
                          log_book=None, stats=None,
                          alg_params=params["alg_params"])

    kwargs = dict(params["executor_params"])
    kwargs.update(params["alg_params"])
    kwargs["ga_params"] = {"population": params["alg_params"]["n"]}

    machine = MPGaHeftOldPopExecutor(heft_planner=dynamic_heft,
                                     wf=_wf,
                                     resource_manager=rm,
                                     estimator=estimator,
                                     stat_saver=None,
                                     ga_builder= lambda: ga,
                                     mpga_builder=lambda: mpga,
                                     chromosome_cleaner=chromosome_cleaner,
                                     mpnewVSmpoldmode=False,
                                     mixed_init_pop=False,
                                     emigrant_selection=None,
                                     check_evolution_for_stopping=False,
                                     schedule_to_chromosome_converter=schedule_to_chromosome_converter_builder(wf, rm, estimator),
                                     **kwargs)

    machine.init()
    machine.run()
    resulted_schedule = machine.current_schedule
    stat_data = machine.executor_stat_data

    Utility.validate_dynamic_schedule(_wf, resulted_schedule)

    data = {
        "wf_name": wf_name,
        "params": params,
        "result": {
            "makespan": Utility.makespan(resulted_schedule),
            ## TODO: this function should be remade to adapt under conditions of dynamic env
            #"overall_transfer_time": Utility.overall_transfer_time(resulted_schedule, _wf, estimator),
            "overall_execution_time": Utility.overall_execution_time(resulted_schedule),
            "overall_failed_tasks_count": Utility.overall_failed_tasks_count(resulted_schedule)
        }
    }

    return data

