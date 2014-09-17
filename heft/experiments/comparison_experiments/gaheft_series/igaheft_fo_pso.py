from functools import partial

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_pfpso, \
    create_pso_cleaner
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_inherited_pop_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import inherited_pop_run


EXPERIMENT_NAME = "igaheft_for_pso"
REPEAT_COUNT = 20
WF_TASKIDS_MAPPING = {
    "Montage_75": ["ID00000_000", "ID00010_000", "ID00020_000", "ID00040_000",
                    "ID00050_000", "ID00070_000"]
}

# WF_TASKIDS_MAPPING = {
#     "Montage_75": ["ID00000_000", "ID00010_000"]
# }

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "pso",
    "alg_params": {
        "w": 0.1,
        "c1": 0.6,
        "c2": 0.2,
        "n": 50,
        "gen_curr": 0,
        "gen_step": 100,
        "is_silent": True
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "task_id_to_fail": "ID00000_000"
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

pso_exp = partial(do_inherited_pop_exp, alg_builder=create_pfpso, chromosome_cleaner_builder=create_pso_cleaner)

# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":
    inherited_pop_run(pso_exp, WF_TASKIDS_MAPPING, REPEAT_COUNT, BASE_PARAMS)
    # test_run(pso_exp, BASE_PARAMS)