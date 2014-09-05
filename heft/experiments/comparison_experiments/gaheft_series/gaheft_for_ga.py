from copy import deepcopy
from functools import partial
import os

from heft.algs.common.algorithm_factory import create_pfga
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import UniqueNameSaver, multi_repeat
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.comparison_experiments.executors.GaHeftExecutor import GaHeftExecutor
from heft.settings import TEMP_PATH


EXPERIMENT_NAME = "gaheft_for_ga"
REPEAT_COUNT = 1

BASE_PARAMS = {
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

def do_exp(wf_name, **params):
    _wf = wf("Montage_100")
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])
    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    ga = create_pfga(_wf, rm, estimator,
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
            "overall_transfer_time": Utility.overall_transfer_time(resulted_schedule, _wf, ESTIMATOR),
            "overall_execution_time": Utility.overall_execution_time(resulted_schedule)
        }
    }

    return data

def test_run():
    configs = []
    # reliability = [1.0, 0.95, 0.9]
    reliability = [1.0]
    wf_name = "Montage_25"
    for r in reliability:
        params = deepcopy(BASE_PARAMS)
        params["estimator_settings"]["reliability"] = r
        configs.append(params)

    to_run = [partial(do_exp, wf_name, **params) for params in configs]
    results = [t() for t in to_run]
    # results = multi_repeat(REPEAT_COUNT, to_run)

    saver = UniqueNameSaver(os.path.join(TEMP_PATH, "gaheft_series"), EXPERIMENT_NAME)
    for result in results:
        saver(result)
    pass

if __name__ == "__main__":
    test_run()
