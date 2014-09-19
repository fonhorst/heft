from functools import partial
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.comparison_experiments.executors.HeftExecutor import HeftExecutor
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, test_run

EXPERIMENT_NAME = "gaheft_for_heft"
REPEAT_COUNT = 200
WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
RELIABILITY = [0.99, 0.975, 0.95, 0.925, 0.9]
INDIVIDUALS_COUNTS = [50]

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "alg_name": "heft",
    "alg_params": {
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fail_count_upper_limit": 15,
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

def heft_exp(wf_name, **params):
    _wf = wf(wf_name)
    rm = ExperimentResourceManager(rg.r(params["resource_set"]["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(**params["estimator_settings"])

    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    heft_machine = HeftExecutor(rm, heft_planner=dynamic_heft,
                                **params["executor_params"])
    heft_machine.init()
    heft_machine.run()
    resulted_schedule = heft_machine.current_schedule

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

if __name__ == "__main__":
    # test_run(heft_exp, BASE_PARAMS)
    changing_reliability_run(heft_exp, RELIABILITY, INDIVIDUALS_COUNTS, REPEAT_COUNT, WF_NAMES, BASE_PARAMS)
