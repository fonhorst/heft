from datetime import datetime
from functools import partial
import uuid

from heft.algs.hs.phs import Env, Specie, vm_run_cooperative_ga
from heft.algs.hs.phs_operators import RESOURCE_CONFIG_SPECIE, GA_SPECIE, \
    vm_resource_default_initialize, resource_conf_crossover, ga_default_initialize, ga_mutate, ga_crossover, \
    resource_config_mutate, fitness_ga_and_vm
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.ResourceManager import ScheduleItem, Schedule
from heft.core.environment.Utility import wf
from heft.core.environment.BladeResourceGenrator import ResourceGenerator as rg
from heft.experiments.cga.utilities.common import repeat
from heft.algs.common.utilities import unzip_result, logbooks_in_data
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.hs.phs_operators import ga2resources_build_schedule
import time
import numpy
from heft.core.vizualization.Vizualization import Vizualizator
from copy import deepcopy
import os
import json


class Config:
    def __init__(self, input_wf_name):

        self.wf_name = input_wf_name
        self._wf = wf(self.wf_name)
        self.rm = ExperimentResourceManager(rg.generate_resources([[30, 25, 15, 10]]))
        self.rm.setVMParameter([(80, 30)])
        self.estimator = ExperimentEstimator(ideal_flops=20, transfer_nodes=100, transfer_blades=100)


        pop_size = 10
        self.config = {
            "generations": 10000,
            "env": Env(self._wf, self.rm, self.estimator),
            "species": [Specie(name=GA_SPECIE, pop_size=pop_size,
                               mate=ga_crossover,
                               mutate=ga_mutate,
                               initialize=ga_default_initialize,
                               ),
                        Specie(name=RESOURCE_CONFIG_SPECIE, pop_size=pop_size,
                               mate=resource_conf_crossover,
                               mutate=resource_config_mutate,
                               initialize=vm_resource_default_initialize,
                               )
                        ],
            "operators": {
                "fitness": fitness_ga_and_vm
            }
        }

def print_sched(schedule):
    result = ""
    for node, values in schedule.mapping.items():
        result += "{0}({1}):\n".format(node.name, node.flops)
        for item in values:
            result += "Start: {0}, end: {1}, job: {2}.\n".format("%0.3f" % item.start_time, "%0.3f" % item.end_time, item.job)
    return result

def resources_printer(resources):
    for res in resources:
        print([node.name + ":" + str(node.flops) for node in res])

def do_experiment(config):

    # create HEFT schedule
    heft_schedule = run_heft(config["env"][0], config["env"][1], config["env"][2])
    config["initial_schedule"] = heft_schedule

    solution, pops, logbook, initial_pops, vm_series = vm_run_cooperative_ga(**config)
    # print("RESULT = ")
    sol_res = ExperimentResourceManager(rg.generate_resources([solution['ResourceConfigSpecie']]))
    sol_res.setVMParameter([(80, 30)])
    # print([(node, node.flops) for node in sol_res.resources[0].nodes])
    # print(solution['GASpecie'])
    config["fixed_schedule"] = Schedule({node: [] for node in sol_res.resources[0].nodes})
    solpair = [solution['GASpecie'], solution['ResourceConfigSpecie']]
    resulted_schedule = ga2resources_build_schedule(config["env"].wf, config["env"].estimator, config["env"].rm, solpair, config)
    #Vizualizator.create_jedule_visualization(resulted_schedule, "D:\Projects\heft_simulator\heft\heft\\vizual", "phs")
    #print("====================Experiment finished========================")

    return solution, logbook

def do_exp(params):

    wf_name = params[1]
    configuration = Config(wf_name)
    config = configuration.config
    res = do_experiment(config)
    return res

if __name__ == "__main__":

    wf_list = ["Montage_25", "Montage_50", "Montage_75", "Montage_100",
               "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
               "Epigenomics_24", "Sipht_30", "Inspiral_30"]
    for wf_name in wf_list:
        print(wf_name.upper())
        repeat_count = 50
        number = uuid.uuid4()
        res = repeat(partial(do_exp, [number, wf_name]), repeat_count)
        result, logbooks = unzip_result(res)
        logbook = logbooks_in_data(logbooks)
        result_json = dict()
        result_json['result'] = -round(numpy.mean([r.fitness for r in result]), 1)
        result_json['iter'] = dict()
        for i in range(int(len(logbook) / 3)):
            result_json['iter'][i] = dict()
            result_json['iter'][i]['min'] = round(logbook[(i, 'min')], 1)
            result_json['iter'][i]['time'] = round(logbook[(i, 'time')], 5)

        path = "D:\papers\Polyrithm\experiments\PHS"
        if not os.path.exists(path):
            os.mkdir(path)
        outfile = open(path + "\\" + wf_name + ".txt", 'w')
        json.dump(result_json, outfile)
        outfile.close()
        print([-r.fitness for r in result])
        print(-numpy.mean([r.fitness for r in result]))








