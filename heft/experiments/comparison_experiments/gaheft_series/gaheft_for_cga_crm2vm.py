from copy import deepcopy
from functools import partial
from deap.base import Toolbox
import functools
from heft.algs.ga.coevolution.cga import Env, Specie, VMCoevolutionGA, vm_run_cooperative_ga
from heft.algs.ga.coevolution.operators import GA_SPECIE, ga_crossover, ga_mutate, ga_default_initialize, \
    RESOURCE_CONFIG_SPECIE, resource_conf_crossover, resource_config_mutate, vm_resource_default_initialize, \
    MutRegulator, one_to_one_vm_build_solutions, fitness_ga_and_vm, max_assign_credits, ga2resources_build_schedule
from heft.experiments.cga.utilities.common import tourn, ArchivedSelector

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_gaheft_exp_for_cga
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, test_run


EXPERIMENT_NAME = "gaheft_for_ga"
REPEAT_COUNT = 2
WF_NAMES = ["Montage_25"]
# WF_NAMES = ["Montage_25"]
# RELIABILITY = [0.99, 0.975, 0.95, 0.925, 0.9]
RELIABILITY = [0.95]
INDIVIDUALS_COUNTS = [5]
# INDIVIDUALS_COUNTS = [60, 105, 150]

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "cga_crm2vm",

    "alg_params": {
            "hall_of_fame_size": 5,
            "interact_individuals_count": 5,
            "generations": 10,
            #"env": Env(self._wf, self.rm, self.estimator),
            "species": [Specie(name=GA_SPECIE, pop_size=50,
                               cxb=0.5, mb=0.5,
                               mate=ga_crossover,
                               mutate=ga_mutate,
                               select=tourn,
                               initialize=ga_default_initialize,
                               ),
                        Specie(name=RESOURCE_CONFIG_SPECIE, pop_size=50,
                               cxb=0.5, mb=0.5,
                               mate=resource_conf_crossover,
                               mutate=resource_config_mutate,
                               select=tourn,
                               initialize=vm_resource_default_initialize,
                               )
                        ],

            "analyzers": [MutRegulator().analyze],

            "operators": {
                # "choose": default_choose,
                # "build_solutions": default_build_solutions,
                "build_solutions": one_to_one_vm_build_solutions(),
                "fitness": fitness_ga_and_vm,
                # "fitness": overhead_fitness_mapping_and_ordering,
                # "assign_credits": default_assign_credits
                # "assign_credits": bonus2_assign_credits
                "assign_credits": max_assign_credits
            }
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "fail_count_upper_limit": 15,
        "replace_anyway": False
    },
    "resource_set": {
        "nodes_conf": [(10, 15, 25, 30)],
        "rules_list": [(80, 30)]
    },
    "estimator_settings": {
        "ideal_flops": 20,
        "transfer_nodes": 100,
        "reliability": 1.0,
        "transfer_blades": 100
    }
}

def create_cga_crm2vm(_wf, rm, estimator,
                     init_sched_percent,
                     log_book, stats,
                     alg_params):

    class CgaVmWrapper:
        def __call__(self, fixed_schedule_part, initial_schedule, current_time=0, initial_population=None):
            kwargs = deepcopy(alg_params)
            kwargs["env"] = Env(_wf, rm, estimator)
            kwargs["fixed_schedule"] = fixed_schedule_part
            kwargs["initial_schedule"] = initial_schedule
            kwargs["current_time"] = current_time
            kwargs["initial_population"] = initial_population

            best, pops, logbook, initial_pops, hall, vm_series = vm_run_cooperative_ga(**kwargs)
            schedule = ga2resources_build_schedule(_wf, estimator, rm, best, ctx=kwargs)
            # logbook = None

            return (best, pops, schedule, None), logbook
            # TODO: debug. Just for test
            # return (None, None, initial_schedule, None), logbook

    return CgaVmWrapper()




ga_exp = partial(do_gaheft_exp_for_cga, alg_builder=create_cga_crm2vm)

if __name__ == "__main__":
    p = deepcopy(BASE_PARAMS)
    # test_run(ga_exp, BASE_PARAMS)
    changing_reliability_run(ga_exp, RELIABILITY, INDIVIDUALS_COUNTS, REPEAT_COUNT, WF_NAMES, BASE_PARAMS, is_debug=True)
