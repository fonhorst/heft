from functools import partial

from heft.experiments.comparison_experiments.gaheft_series.algorithm_factory import create_pfpso
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, \
    do_inherited_pop_exp, test_run



EXPERIMENT_NAME = "igaheft_for_pso"
REPEAT_COUNT = 10
WF_NAMES = ["Montage_40"]
RELIABILITY = [0.99, 0.975, 0.95, 0.925, 0.9]

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "pso",
    "alg_params": {
        "w": 0.1,
        "c1": 0.6,
        "c2": 0.2,
        "n": 10,
        "gen_curr": 0,
        "gen_step": 30,

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

pso_exp = partial(do_inherited_pop_exp, alg_builder=create_pfpso)

# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":
    # TODO: Unworked version, it needs to be reworked properly. e.g. different experimental conditions should be added
    raise NotImplementedError("Unworked version, it needs to be reworked properly. e.g. different experimental conditions should be added ")
    # profile_test_run(pso_exp, BASE_PARAMS)
    test_run(pso_exp, BASE_PARAMS)
    # changing_reliability_run(pso_exp, RELIABILITY, REPEAT_COUNT, WF_NAMES, BASE_PARAMS)