from functools import partial
from heft.algs.pso.ordering_operators import generate

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_pfpso, create_pso_cleaner, create_pfmpga, \
    create_schedule_to_pso_chromosome_converter, create_pso_alg
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_island_inherited_pop_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import inherited_pop_run


EXPERIMENT_NAME = "migaheft_for_pso"
REPEAT_COUNT = 1
# WF_TASKIDS_MAPPING = {
#     "Montage_100": ["ID00000_000", "ID00010_000", "ID00020_000", "ID00040_000",
#                     "ID00050_000", "ID00070_000", "ID00090_000"]
# }

WF_TASKIDS_MAPPING = {
    "Montage_100": ["ID00000_000"]
}

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "pso",
    "alg_params": {
        "w": 0.1,
        "c1": 0.6,
        "c2": 0.2,
        "n": 5,
        "gen_curr": 0,
        "gen_step": 4,
        "is_silent": True,
        "migrCount": 1,
        "all_iters_count": 30,
        "merged_pop_iters": 10,
        "generations_count_before_merge": 20,
        "generations_count_after_merge": 10,
        "migrationInterval": 5,

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


ga_exp = partial(do_island_inherited_pop_exp, alg_builder=create_pfpso, algorithm_builder=partial(create_pso_alg, generate_=lambda n: None), mp_alg_builder=create_pfmpga,
                 chromosome_cleaner_builder=create_pso_cleaner,
                 schedule_to_chromosome_converter_builder=create_schedule_to_pso_chromosome_converter)

# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":
    inherited_pop_run(ga_exp, WF_TASKIDS_MAPPING, REPEAT_COUNT, BASE_PARAMS)
    # test_run(ga_exp, BASE_PARAMS)
