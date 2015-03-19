from datetime import datetime
from functools import partial
import uuid

from heft.algs.ga.coevolution.cga import Env, Specie, vm_run_cooperative_ga
from heft.algs.ga.coevolution.operators import RESOURCE_CONFIG_SPECIE, GA_SPECIE, \
    vm_resource_default_initialize, resource_conf_crossover, ga_default_initialize, ga_mutate, ga_crossover, \
    max_assign_credits, MutRegulator, resource_config_mutate, \
    one_to_one_vm_build_solutions, fitness_ga_and_vm
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.Utility import wf
from heft.core.environment.BladeResourceGenrator import ResourceGenerator as rg
from heft.experiments.cga.utilities.common import BasicFinalResultSaver, repeat, tourn, ArchivedSelector, \
    extract_mapping_from_ga_file, extract_ordering_from_ga_file
from heft.algs.common.utilities import unzip_result
import numpy
import os


class Config:
    def __init__(self, input_wf_name):

        self.wf_name = input_wf_name
        self._wf = wf(self.wf_name)
        # input changed from 1 dimension list to 2 dimensions
        self.rm = ExperimentResourceManager(rg.generate_resources([[10, 15, 25, 30], [15, 15, 20, 30]]))
        self.rm.setVMParameter([(80, 30), (80, 30)])
        # now transfer time less, if nodes from one blade
        self.estimator = ExperimentEstimator(ideal_flops=20, transfer_nodes=1, transfer_blades=100)

        self.mapping_selector = ArchivedSelector(3)(tourn)
        self.ordering_selector = ArchivedSelector(3)(tourn)

        self.mapping_mut_reg = MutRegulator()

        self.config = {
            "hall_of_fame_size": 5,
            "interact_individuals_count": 200,
            "generations": 10,
            "env": Env(self._wf, self.rm, self.estimator),
            "species": [Specie(name=GA_SPECIE, pop_size=100,
                               cxb=0.9, mb=0.9,
                               mate=ga_crossover,
                               mutate=ga_mutate,
                               select=self.mapping_selector,
                               initialize=ga_default_initialize,
                               ),
                        Specie(name=RESOURCE_CONFIG_SPECIE, pop_size=100,
                               cxb=0.9, mb=0.9,
                               mate=resource_conf_crossover,
                               mutate=resource_config_mutate,
                               select=self.ordering_selector,
                               initialize=vm_resource_default_initialize,
                               )
                        ],

            "analyzers": [self.mapping_mut_reg.analyze],

            "operators": {
                # "choose": default_choose,
                # "build_solutions": default_build_solutions,
                "build_solutions": one_to_one_vm_build_solutions,
                "fitness": fitness_ga_and_vm,
                # "fitness": overhead_fitness_mapping_and_ordering,
                # "assign_credits": default_assign_credits
                # "assign_credits": bonus2_assign_credits
                "assign_credits": max_assign_credits
            }
        }


def print_sched(schedule):
    result = ""
    for node, values in schedule.mapping.items():
        result += "{0}({1}):\n".format(node.name, node.flops)
        for item in values:
            result += "Start: {0}, end: {1}, job: {2}.\n".format("%0.3f" % item.start_time, "%0.3f" % item.end_time, item.job)
    return result

def do_experiment(saver, config, number):
    solution, pops, logbook, initial_pops, hall, vm_series = vm_run_cooperative_ga(**config)
    #print("====================Experiment finished========================")

    max_value = max(hall.keys)

    data = {
        "final_makespan": max_value,
        "vm_series": vm_series,
        "final_resources": hall.items[len(hall.items) - 1][RESOURCE_CONFIG_SPECIE]
    }

    # TODO now doen't need
    #saver(data, number, config)

    return max_value, logbook

def do_exp(params):

    # TODO: remove time measure
    tstart = datetime.now()

    number = params[0]
    wf_name = params[1]

    basic_saver = BasicFinalResultSaver("../../den_temp/coev_ga_vm_"+wf_name+"_exp")

    configuration = Config(wf_name)
    config = configuration.config
    for s in config["species"]:
        s.select = ArchivedSelector(3)(tourn)
    res = do_experiment(basic_saver, config, number)

    tend = datetime.now()
    tres = tend - tstart
    print("Time Result: " + str(tres.total_seconds()))
    return res

def logbook_to_bests(logbook):
    result = []
    for gen in logbook:
        result.append(-gen['popsstat'][0]['GASpecie']['best'])
    return result

def logbooks_reduce(logbooks):
    result = {}
    for book in logbooks:
        for i in range(len(book)):
            if i not in result.keys():
                result[i] = book[i]
            else:
                result[i] += book[i]
    for i in range(len(result)):
        result[i] = result[i] / len(logbooks)
    return result

def logs_to_file(logs, dir, wf_name, comment=""):
    path = dir + "cga_" + wf_name.lower() + ".txt"
    file = open(path, 'w')
    file.write("#" + comment + "\n")
    for i in range(len(logs)):
        file.write(str(i) + "\t" + str(logs[i]) + "\n")
    file.close()

if __name__ == "__main__":

    wf_names = [
                "Montage_25", "Montage_50"
                ]
    dir = "./cga_results/"
    repeat_count = 1

    for wf_name in wf_names:
        print("++++++========++++++++")
        print(wf_name.upper())
        print("")
        number = uuid.uuid4()
        res, logbooks = unzip_result(repeat(partial(do_exp, [number, wf_name]), repeat_count))

        # Output to file
        logs = []
        for logbook in logbooks:
            logs.append(logbook_to_bests(logbook))
        res_log = logbooks_reduce(logs)
        logs_to_file(res_log, dir, wf_name)

        print("RESULTS: ")
        print(-numpy.mean(res))
        print("")






