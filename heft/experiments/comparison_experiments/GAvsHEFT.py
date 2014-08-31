import functools
import json
import os
import numpy

from heft.algs.ga.GAImplementation.GARunner import MixRunner
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.cga.utilities.common import UniqueNameSaver, repeat
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg


wf_names = ['CyberShake_30']
# wf_names = ['Montage_50']
# wf_names = ['Montage_500']
# wf_names = ['CyberShake_100']
# wf_names = ['Epigenomics_100']
# wf_names = ["CyberShake_50"]

only_heft = False

PARAMS = {
    "ideal_flops": 20,
    "is_silent": False,
    "is_visualized": False,
    "ga_params": {
        "Kbest": 5,
        "population": 50,
        "crossover_probability": 0.3, #0.8
        "replacing_mutation_probability": 0.1, #0.5
        "sweep_mutation_probability": 0.3, #0.4
        "generations": 300
    },
    "nodes_conf": [10, 15, 25, 30],
    "transfer_time": 2000,
    "heft_initial": True
}

run = functools.partial(MixRunner(), **PARAMS)
directory = "../../temp/ga_vs_heft_exp"
saver = UniqueNameSaver("../../temp/ga_vs_heft_exp")

# def do_exp():
#     ga_makespan, heft_makespan, ga_schedule, heft_schedule = run(wf_names[0])
#     saver(ga_makespan)
#     return ga_makespan

def do_exp_schedule(takeHeftSchedule=True):
    saver = UniqueNameSaver("../../temp/ga_vs_heft_exp_heft_schedule")

    ga_makespan, heft_makespan, ga_schedule, heft_schedule = run(wf_names[0])

    ## TODO: pure hack

    schedule = heft_schedule if takeHeftSchedule else ga_schedule

    mapping = [(item.job.id, node.flops) for node, items in schedule.mapping.items() for item in items]
    mapping = sorted(mapping, key=lambda x: x[0])

    ordering = [(item.job.id, item.start_time) for node, items in heft_schedule.mapping.items() for item in items]
    ordering = [t for t, time in sorted(ordering, key=lambda x: x[1])]

    data = {
        "mapping": mapping,
        "ordering": ordering
    }

    name = saver(data)
    return ga_makespan, heft_makespan, ga_schedule, heft_schedule, name

def do_exp_heft_schedule():
    res = do_exp_schedule(True)
    return res[0]

def do_exp_ga_schedule():
    res = do_exp_schedule(False)
    return (res[0], res[4])


if __name__ == '__main__':
    print("Population size: " + str(PARAMS["ga_params"]["population"]))

    _wf = wf(wf_names[0])
    rm = ExperimentResourceManager(rg.r(PARAMS["nodes_conf"]))
    estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=PARAMS["ideal_flops"], transfer_time=PARAMS["transfer_time"])

    heft_schedule = run_heft(_wf, rm, estimator)
    heft_makespan = Utility.makespan(heft_schedule)
    overall_transfer = Utility.overall_transfer_time(heft_schedule, _wf, estimator)
    overall_execution = Utility.overall_execution_time(heft_schedule)

    print("Heft makespan: {0}, Overall transfer time: {1}, Overall execution time: {2}".format(heft_makespan,
                                                                                               overall_transfer,
                                                                                               overall_execution))

    if not only_heft:
        result = repeat(do_exp_heft_schedule, 10)
        mean = numpy.mean(result)
        profit = (1 - mean / heft_makespan) * 100
        print(result)
        print("Heft makespan: {0}, Overall transfer time: {1}, Overall execution time: {2}".format(heft_makespan,
                                                                                               overall_transfer,
                                                                                               overall_execution))
        print("Mean: {0}".format(mean))
        print("Profit: {0}".format(profit))


