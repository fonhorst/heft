from copy import deepcopy
from functools import partial
import os
from timeit import timeit
from heft.algs.common.NewSchedulerBuilder import NewScheduleBuilder
from heft.algs.common.individuals import DictBasedIndividual
from heft.algs.ga.GAImplementation.GAFunctions2 import unmoveable_tasks
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.algs.pso.ompso import CompoundParticle, numseq_to_ordering
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import UniqueNameSaver, multi_repeat
from heft.experiments.comparison_experiments.executors.GaHeftExecutor import GaHeftExecutor
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.comparison_experiments.executors.GaHeftOldPopExecutor import GaHeftOldPopExecutor
from heft.settings import TEMP_PATH

EXAMPLE_BASE_PARAMS = {
    "experiment_name": None,
    "init_sched_percent": 0.05,
    "alg_name": "ga",
    "alg_params": {
        "kbest": 5,
        "n": 10,
        "cxpb": 0.3,  # 0.8
        "mutpb": 0.1,  # 0.5
        "sweepmutpb": 0.3,  # 0.4
        "gen_curr": 0,
        "gen_step": 30,
        "is_silent": True
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15
    },
    "resource_set": {
        "nodes_conf": [10, 15, 25, 30],
    },
    "estimator_settings": {
        "transferMx": None,
        "comp_time_cost": 0,
        "transf_time_cost": 0,
        "ideal_flops": 20,
        "transfer_time": 100,
        "reliability": 1.0
    }
}


class ParticleScheduleBuilder(NewScheduleBuilder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._unmoveable_tasks = unmoveable_tasks(self.fixed_schedule_part)
        pass


    def _particle_to_chromo(self, particle):
        """
        Converts Particle representation of individual to chromosome representation used by GA operators
        """
        if isinstance(particle, CompoundParticle):
            ordering = numseq_to_ordering(self.workflow, particle, self._unmoveable_tasks)
            chromo_mapping = {node.name: [] for node in self.nodes}
            for task_id in ordering:
                node_name = particle.mapping.entity[task_id]
                chromo_mapping[node_name].append(task_id)
                pass
            chromo = DictBasedIndividual(chromo_mapping)
            return chromo
        raise ValueError("particle has a wrong type: {0}".format(type(particle)))

    def __call__(self, particle, current_time):
        chromo = self._particle_to_chromo(particle)
        result = super().__call__(chromo, current_time)
        return result

    pass


def do_exp(alg_builder, wf_name, **params):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    ga = alg_builder(_wf, rm, estimator,
                     params["init_sched_percent"],
                     logbook=None, stats=None,
                     **params["alg_params"])
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

    return data


def do_inherited_pop_exp(alg_builder, wf_name, **params):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    ga = alg_builder(_wf, rm, estimator,
                     params["init_sched_percent"],
                     logbook=None, stats=None,
                     **params["alg_params"])
    machine = GaHeftOldPopExecutor(heft_planner=dynamic_heft,
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

    return data


def test_run(exp, base_params):
    configs = []
    # reliability = [1.0, 0.95, 0.9]
    # reliability = [1.0]
    reliability = [0.99]
    wf_name = "Montage_25"
    for r in reliability:
        params = deepcopy(base_params)
        params["estimator_settings"]["reliability"] = r
        configs.append(params)

    to_run = [partial(exp, wf_name=wf_name, **params) for params in configs]
    results = [t() for t in to_run]
    # results = multi_repeat(REPEAT_COUNT, to_run)

    saver = UniqueNameSaver(os.path.join(TEMP_PATH, "gaheft_series"), base_params["experiment_name"])
    for result in results:
        saver(result)
    pass


def changing_reliability_run(exp, reliability, repeat_count, wf_names, base_params):
    configs = []
    for r in reliability:
        params = deepcopy(base_params)
        params["estimator_settings"]["reliability"] = r
        configs.append(params)

    to_run = [partial(exp, wf_name=wf_name, **params) for wf_name in wf_names for params in configs]
    results = multi_repeat(repeat_count, to_run)

    saver = UniqueNameSaver(os.path.join(TEMP_PATH, "gaheft_series"), base_params["experiment_name"])
    for result in results:
        saver(result)
    pass
