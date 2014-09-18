from functools import partial
from heft.algs.pso.ordering_operators import generate

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_pfpso, create_pso_cleaner, create_pfmpga, \
    create_schedule_to_pso_chromosome_converter, create_pso_alg
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_island_inherited_pop_exp, \
    do_triple_island_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import inherited_pop_run, changing_reliability_run, \
    SaveToDirectory


EXPERIMENT_NAME = "migaheft_for_pso"
REPEAT_COUNT = 50
WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
RELIABILITY = [0.95]
INDIVIDUALS_COUNTS = [20, 35, 50]


BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.00,
    "alg_name": "pso",
    "alg_params": {
        "w": 0.1,
        "c1": 0.6,
        "c2": 0.2,
        "n": 50,#50,
        ## param for init run
        "gen_curr": 0,
        ## param for init run
        "gen_step": 30,#300,

        "is_silent": True,
        "migrCount": 5,
        #"all_iters_count": 300,
        #"merged_pop_iters": 100,
        "generations_count_before_merge": 200,#200
        "generations_count_after_merge": 100,#100
        "migrationInterval": 10,

    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "fail_count_upper_limit": 15,
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


ga_exp = partial(do_triple_island_exp, alg_builder=partial(create_pfmpga, algorithm=create_pso_alg, generate_func=generate),
                 chromosome_cleaner_builder=create_pso_cleaner,
                 schedule_to_chromosome_converter_builder=create_schedule_to_pso_chromosome_converter)


# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":
    changing_reliability_run(ga_exp, RELIABILITY, INDIVIDUALS_COUNTS, REPEAT_COUNT, WF_NAMES, BASE_PARAMS)
    # test_run(ga_exp, BASE_PARAMS)
