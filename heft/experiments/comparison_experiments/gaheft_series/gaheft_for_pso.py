from functools import partial
from heft.core.environment.Utility import profile_decorator

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_pfpso
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_gaheft_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import test_run, changing_reliability_run


EXPERIMENT_NAME = "gaheft_for_pso"
REPEAT_COUNT = 200
WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
RELIABILITY = [0.99, 0.975, 0.95, 0.925, 0.9]
INDIVIDUALS_COUNTS = [50]
# INDIVIDUALS_COUNTS = [60, 105, 150]

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.00,
    "alg_name": "pso",
    "alg_params": {
        "w": 0.1,
        "c1": 0.6,
        "c2": 0.2,
        "n": 50,
        "gen_curr": 0,
        "gen_step": 300,

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

pso_exp = partial(do_gaheft_exp, alg_builder=create_pfpso)

# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":

    # profile_test_run(pso_exp, BASE_PARAMS)
    # test_run(pso_exp, BASE_PARAMS)
    changing_reliability_run(pso_exp, RELIABILITY, INDIVIDUALS_COUNTS, REPEAT_COUNT, WF_NAMES, BASE_PARAMS)
