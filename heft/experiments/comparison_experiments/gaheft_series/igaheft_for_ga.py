from copy import deepcopy
from functools import partial
import scoop
import sys
import heft

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga, create_ga_cleaner
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_inherited_pop_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import test_run, \
    inherited_pop_run
from settings import TEMP_PATH

if scoop.IS_RUNNING:
    from scoop import futures
    map_func = futures.map
else:
    map_func = map
    heft.experiments.cga.utilities.common.USE_SCOOP = False

EXPERIMENT_NAME = "igaheft_for_ga"

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "ga",
    "alg_params": {
        "kbest": 5,
        "n": 50,
        "cxpb": 0.8,  # 0.8
        "mutpb": 0.6,  # 0.5
        "sweepmutpb": 0.5,  # 0.4
        "gen_curr": 0,
        "gen_step": 200,
        "is_silent": True
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "task_id_to_fail": "ID00000"
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

ga_exp = partial(do_inherited_pop_exp, alg_builder=create_old_ga, chromosome_cleaner_builder=create_ga_cleaner)

# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        mode = "normal"

    if mode == 'test':
        REPEAT_COUNT = 1
        WF_TASKIDS_MAPPING = {
            "Montage_75": ["ID00020_000_Montage_75"]
        }
        exp_params = deepcopy(BASE_PARAMS)
        exp_params["alg_params"]["n"] = 6
        exp_params["alg_params"]["gen_step"] = 5
    else:
        REPEAT_COUNT = 50
        exp_params = BASE_PARAMS
        WF_TASKIDS_MAPPING = {
            "Montage_75": ["ID00000_000_Montage_75", "ID00010_000_Montage_75", "ID00020_000_Montage_75", "ID00040_000_Montage_75",
                            "ID00050_000_Montage_75", "ID00070_000_Montage_75"]
        }

    inherited_pop_run(ga_exp,
                      WF_TASKIDS_MAPPING,
                      REPEAT_COUNT,
                      exp_params,
                      path_to_save=TEMP_PATH + "/new_igaheft_ga")

