from copy import deepcopy
from functools import partial
import scoop
import sys
import heft

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_gaheft_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, test_run
from settings import TEMP_PATH

if scoop.IS_RUNNING:
    from scoop import futures
    map_func = futures.map
else:
    map_func = map
    heft.experiments.cga.utilities.common.USE_SCOOP = False

EXPERIMENT_NAME = "gaheft_for_ga"

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "ga",
    "alg_params": {
        "kbest": 5,
        "n": 50,
        "cxpb": 0.3,  # 0.8
        "mutpb": 0.1,  # 0.5
        "sweepmutpb": 0.3,  # 0.4
        "gen_curr": 0,
        "gen_step": 200,
        "is_silent": True
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "fail_count_upper_limit": 15,
        "replace_anyway": False
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

ga_exp = partial(do_gaheft_exp, alg_builder=create_old_ga)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        mode = "normal"

    if mode == 'test':
        REPEAT_COUNT = 1
        WF_NAMES = ["Montage_25"]
        RELIABILITY = [0.9]
        INDIVIDUALS_COUNTS = [6]
        exp_params = deepcopy(BASE_PARAMS)
        exp_params["alg_params"]["n"] = 6
        exp_params["alg_params"]["gen_step"] = 5
    else:
        REPEAT_COUNT = 25
        WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
        RELIABILITY = [0.99, 0.975, 0.95, 0.925, 0.9]
        INDIVIDUALS_COUNTS = [50]
        exp_params = BASE_PARAMS

    # profile_test_run(pso_exp, BASE_PARAMS)
    # test_run(pso_exp, BASE_PARAMS)
    changing_reliability_run(ga_exp,
                             RELIABILITY,
                             INDIVIDUALS_COUNTS,
                             REPEAT_COUNT,
                             WF_NAMES,
                             exp_params,
                             path_to_save=TEMP_PATH + "/new_gaheft_ga")
