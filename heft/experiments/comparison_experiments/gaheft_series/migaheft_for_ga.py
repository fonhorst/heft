from functools import partial
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga, create_ga_cleaner, \
    create_old_pfmpga, create_schedule_to_ga_chromosome_converter
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_island_inherited_pop_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import inherited_pop_run


EXPERIMENT_NAME = "migaheft_for_ga"
REPEAT_COUNT = 20
WF_TASKIDS_MAPPING = {
    "Montage_75": ["ID00000_000", "ID00010_000", "ID00020_000", "ID00040_000",
                    "ID00050_000", "ID00070_000"]
}

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "pso",
    "alg_params": {
        "kbest": 5,
        "n": 50,
        "cxpb": 0.3,  # 0.8
        "mutpb": 0.1,  # 0.5
        "sweepmutpb": 0.3,  # 0.4
        "gen_curr": 0,
        "gen_step": 300,
        "is_silent": True,
        "merged_pop_iters": 100,
        "migrCount": 5,
        "all_iters_count": 300
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

def algorithm_builder(**kwargs):
    return None

ga_exp = partial(do_island_inherited_pop_exp, alg_builder=create_old_ga, mp_alg_builder=create_old_pfmpga, algorithm_builder=algorithm_builder,
                 chromosome_cleaner_builder=create_ga_cleaner, schedule_to_chromosome_converter_builder=create_schedule_to_ga_chromosome_converter)

# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":
    inherited_pop_run(ga_exp, WF_TASKIDS_MAPPING, REPEAT_COUNT, BASE_PARAMS)
    # test_run(ga_exp, BASE_PARAMS)