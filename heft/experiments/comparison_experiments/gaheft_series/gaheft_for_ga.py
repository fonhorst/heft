from copy import deepcopy
from functools import partial
import os


from heft.experiments.cga.utilities.common import UniqueNameSaver, multi_repeat
from heft.experiments.comparison_experiments.gaheft_series.algorithm_factory import create_pfga
from heft.experiments.comparison_experiments.gaheft_series.utilities import do_exp, test_run, changing_reliability_run
from heft.settings import TEMP_PATH


EXPERIMENT_NAME = "gaheft_for_ga"
REPEAT_COUNT = 10
WF_NAMES = ["Montage_25"]

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
        "gen_step": 300,
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

ga_exp = partial(do_exp, alg_builder=create_pfga)

if __name__ == "__main__":
    # test_run(ga_exp, BASE_PARAMS)
    changing_reliability_run(ga_exp, REPEAT_COUNT, WF_NAMES, BASE_PARAMS)
