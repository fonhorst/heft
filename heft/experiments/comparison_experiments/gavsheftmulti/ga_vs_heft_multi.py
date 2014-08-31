from copy import deepcopy
from functools import partial
from heft.algs.ga.GAImplementation.GARunner import MixRunner
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import multi_repeat, UniqueNameSaver
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.settings import TEMP_PATH

## TODO: node name mapping (single for ga and heft, for schedule representation)

EXPERIMENT_NAME = "ga_vs_heft_multi"

BASE_PARAMS = {
    "ideal_flops": 20,
    "is_silent": True,
    "is_visualized": False,
    "ga_params": {
        "Kbest": 5,
        "population": 50,
        "crossover_probability": 0.3,  # 0.8
        "replacing_mutation_probability": 0.1,  # 0.5
        "sweep_mutation_probability": 0.3,  # 0.4
        "generations": 300
    },
    "nodes_conf": [10, 15, 25, 30],
    "transfer_time": 100,
    "heft_initial": True
}




def copy_and_set(params, **kwargs):
    new_params = deepcopy(params)
    for name, val in kwargs.items():
        new_params[name] = val
    return new_params


def make_linear_sequence(base_params, params):
    seq = [copy_and_set(base_params, **{name: val}) for name, arr in params.items() for val in arr]
    return seq


def do_exp(wf_name, **params):

    _wf = wf(wf_name)

    ga_makespan, heft_make, ga_schedule, heft_sched = MixRunner()(_wf, **params)

    rm = ExperimentResourceManager(rg.r(params["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=params["ideal_flops"],
                                    transfer_time=params["transfer_time"])

    heft_schedule = run_heft(_wf, rm, estimator)
    heft_makespan = Utility.makespan(heft_schedule)


    data = {
        "wf_name": wf_name,
        "params": BASE_PARAMS,
        "heft": {
            "makespan": heft_makespan,
            "overall_transfer_time": Utility.overall_transfer_time(heft_schedule, _wf, estimator),
            "overall_execution_time": Utility.overall_execution_time(heft_schedule)
        },
        "ga": {
            "makespan": ga_makespan,
            "overall_transfer_time": Utility.overall_transfer_time(ga_schedule, _wf, estimator),
            "overall_execution_time": Utility.overall_execution_time(ga_schedule)
        },
        #"heft_schedule": heft_schedule,
        #"ga_schedule": ga_schedule
    }

    return data

def test_run():
    wf_names = ['Montage_25', 'CyberShake_30', 'Inspiral_30', 'Sipht_30', 'Epigenomics_24']

    BASE_PARAMS["ga_params"]["population"] = 5
    BASE_PARAMS["ga_params"]["Kbest"] = 1
    BASE_PARAMS["ga_params"]["generations"] = 10

    seq = make_linear_sequence(BASE_PARAMS, {
        "transfer_time": [0, 10, 50, 100, 300, 500, 2000],
        "ideal_flops": [1, 20, 50, 100, 500]
    })

    to_run = [partial(do_exp, wf_name, **params) for wf_name in wf_names for params in seq]
    results = [t() for t in to_run for _ in range(3)]
    saver = UniqueNameSaver(TEMP_PATH, EXPERIMENT_NAME)
    for result in results:
        saver(result)
    pass

def real_run():
    wf_names = ['Montage_25', 'CyberShake_30', 'Inspiral_30', 'Sipht_30', 'Epigenomics_24']
    seq = make_linear_sequence(BASE_PARAMS, {
        "transfer_time": [0, 10, 50, 100, 300, 500, 2000],
        "ideal_flops": [1, 20, 50, 100, 500]
    })

    to_run = [partial(do_exp, wf_name, **params) for wf_name in wf_names for params in seq]
    results = multi_repeat(10, to_run)
    saver = UniqueNameSaver(TEMP_PATH, EXPERIMENT_NAME)
    for result in results:
        saver(result)
    pass

if __name__ == '__main__':
    # test_run()
    real_run()
    pass