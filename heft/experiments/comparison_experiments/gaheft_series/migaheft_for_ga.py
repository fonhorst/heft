from copy import deepcopy
from functools import partial
import scoop
import sys
import heft
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga, create_ga_cleaner, \
    create_old_pfmpga, create_schedule_to_ga_chromosome_converter
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_island_inherited_pop_exp, \
    do_triple_island_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import inherited_pop_run, changing_reliability_run, \
    SaveToDirectory
from settings import TEMP_PATH

if scoop.IS_RUNNING:
    from scoop import futures
    map_func = futures.map
else:
    map_func = map
    heft.experiments.cga.utilities.common.USE_SCOOP = False

EXPERIMENT_NAME = "migaheft_for_ga"

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "ga",
    "alg_params": {
        "kbest": 5,
        "n": 50,#50,
        "cxpb": 0.8,  # 0.8
        "mutpb": 0.6,  # 0.5
        "sweepmutpb": 0.5,  # 0.4
        "gen_curr": 0,
        "gen_step": 10,#300,
        "is_silent": True,
        "merged_pop_iters": 50,
        "migrCount": 5,
        "all_iters_count": 200
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "task_id_to_fail": "ID00000",
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

ga_exp = partial(do_triple_island_exp, alg_builder=create_old_pfmpga,
                           chromosome_cleaner_builder=create_ga_cleaner,
                           schedule_to_chromosome_converter_builder=create_schedule_to_ga_chromosome_converter)


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
                             path_to_save=TEMP_PATH + "/new_migaheft_ga")
