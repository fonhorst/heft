from functools import partial

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_gaheft_exp, do_reduced_gaheft_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, test_run, \
    changing_tasks_run


EXPERIMENT_NAME = "reduced_gaheft_for_ga"
REPEAT_COUNT = 1
WF_NAMES = ["Montage_100"]
INDIVIDUALS_COUNTS = [50]

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
        "base_fail_duration": -1,
        "base_fail_dispersion": 1, # it means failed node will not go up later.
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

ga_exp = partial(do_reduced_gaheft_exp, alg_builder=create_old_ga)

if __name__ == "__main__":
    
    changing_tasks_run(ga_exp, INDIVIDUALS_COUNTS, REPEAT_COUNT, WF_NAMES, BASE_PARAMS, is_debug=True)
