from copy import deepcopy
from functools import partial
from heft.algs.ga.GAImplementation.GARunner import MixRunner
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import multi_repeat, UniqueNameSaver
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.comparison_experiments.gavsheftmulti.coeff_aggregator import aggregate as coeff_aggregate
from heft.settings import TEMP_PATH

## TODO: node name mapping (single for ga and heft, for schedule representation)


REPEAT_COUNT = 100
EXPERIMENT_NAME = "coeff_cyber_epig_100runs_100pop_{0}".format(REPEAT_COUNT)

BASE_PARAMS = {
    "ideal_flops": 20,
    "is_silent": True,
    "is_visualized": False,
    "ga_params": {
        "Kbest": 5,
        "population": 100,
        "crossover_probability": 0.3,  # 0.8
        "replacing_mutation_probability": 0.1,  # 0.5
        "sweep_mutation_probability": 0.3,  # 0.4
        "generations": 300
    },
    "nodes_conf": [10, 15, 25, 30],
    "transfer_time": 100,
    "heft_initial": True,
    ## it is needed for coeff_run
    "data_intensive_coeff": None
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
        "params": params,
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
    results = multi_repeat(REPEAT_COUNT, to_run)
    saver = UniqueNameSaver(TEMP_PATH, EXPERIMENT_NAME)
    for result in results:
        saver(result)
    pass


def coeff_run():
    """
    coefficient of compute/data intensivity
    """

    # wf_names = ['Montage_25', 'CyberShake_30', 'Inspiral_30', 'Sipht_30', 'Epigenomics_24']
    #all_coeffs = [1/100, 1/50, 1/10, 1/5, 1/2.766, 15, 20, 25, 30, 35, 40, 45, 75]
    all_coeffs = [1/100, 1/50, 1/10, 1/5, 1/2.766, 1, 2.766] + list(range(5, 101, 5))

    # wf_names = [('Montage_25', [10]),
    #             ('CyberShake_30', [0.1] + list(range(10, 46, 1))),
    #             ('Inspiral_30', [10, 1, 1/2.766]),
    #             ('Sipht_30', [0.02] + list(range(30, 50, 1)) + list(range(50, 101, 5))),
    #             ('Epigenomics_24', list(range(5, 46, 1)) + list(range(45, 101, 5)))]

    # wf_names = [('Montage_25', [2.766])]

    wf_names = [#('Montage_25', [10]),
                ('CyberShake_30', all_coeffs),
                #('Inspiral_30', [10, 1, 1/2.766]),
                #('Sipht_30', [0.02] + list(range(30, 50, 1)) + list(range(50, 101, 5))),
                ('Epigenomics_24', all_coeffs)]


    def transfer_time(max_runtime, c):
        transfer = max_runtime * BASE_PARAMS["ideal_flops"] / c
        return transfer

    to_run = []
    for wf_name, coeffs in wf_names:
        _wf = wf(wf_name)
        max_runtime = max(_wf.get_all_unique_tasks(), key=lambda x: x.runtime).runtime
        param_sets = [copy_and_set(BASE_PARAMS, transfer_time=transfer_time(max_runtime, c), data_intensive_coeff=c) for c in coeffs]
        exps = [partial(do_exp, wf_name, **params) for params in param_sets]
        to_run = to_run + exps

    m_repeat = lambda n, funcs: [f() for f in funcs for _ in range(n)]
    results = m_repeat(REPEAT_COUNT, to_run)
    # results = multi_repeat(REPEAT_COUNT, to_run)
    saver = UniqueNameSaver(TEMP_PATH, EXPERIMENT_NAME)
    for result in results:
        saver(result)

    # coeff_aggregate(saver.directory, "coeff.png")
    pass

if __name__ == '__main__':
    # test_run()
    # real_run()
    coeff_run()
    pass