from copy import deepcopy
from functools import partial
import sys

import scoop

import heft
from heft.algs.pso.ordering_operators import generate
from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_pso_cleaner, create_pfmpga, \
    create_schedule_to_pso_chromosome_converter, create_pso_alg
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_triple_island_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, \
    SaveToDirectory
from settings import TEMP_PATH

if scoop.IS_RUNNING:
    from scoop import futures
    map_func = futures.map
else:
    map_func = map
    heft.experiments.cga.utilities.common.USE_SCOOP = False

EXPERIMENT_NAME = "migaheft_for_pso"

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "pso",
    "alg_params": {
        "w": 0.6,
        "c1": 1.4,
        "c2": 1.2,
        "n": 50, # 50,
        # param for init run
        "gen_curr": 0,
        # param for init run
        "gen_step": 200, # 300,
        "is_silent": True,
        "migrCount": 5,
        # "all_iters_count": 300,
        # "merged_pop_iters": 100,
        "generations_count_before_merge": 150, # 200
        "generations_count_after_merge": 50, # 100
        "migrationInterval": 10,

    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "fail_count_upper_limit": 15,
        "task_id_to_fail": "ID00000",
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


ga_exp = partial(do_triple_island_exp,
                 alg_builder=partial(create_pfmpga,
                                     algorithm=create_pso_alg,
                                     generate_func=generate),
                 chromosome_cleaner_builder=create_pso_cleaner,
                 schedule_to_chromosome_converter_builder=create_schedule_to_pso_chromosome_converter)


# profile_test_run = profile_decorator(test_run)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        mode = "normal"

    if mode == 'test':
        REPEAT_COUNT = 500
        WF_NAMES = ["Montage_25"]
        RELIABILITY = [0.9]
        INDIVIDUALS_COUNTS = [6]
        exp_params = deepcopy(BASE_PARAMS)
        exp_params["alg_params"]["n"] = 6
        exp_params["alg_params"]["gen_step"] = 5
        exp_params["alg_params"]["generations_count_before_merge"] = 10
        exp_params["alg_params"]["generations_count_after_merge"] = 5
    else:
        REPEAT_COUNT = 50
        WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
        RELIABILITY = [0.95]
        INDIVIDUALS_COUNTS = [20, 35, 50]
        exp_params = BASE_PARAMS

    changing_reliability_run(ga_exp,
                             RELIABILITY,
                             INDIVIDUALS_COUNTS,
                             REPEAT_COUNT,
                             WF_NAMES,
                             exp_params,
                             path_to_save=TEMP_PATH + "/new_migaheft_pso")
