from copy import deepcopy
from functools import partial
from deap import tools
from deap.base import Toolbox
import numpy
from heft.algs.ga.GAImplementation.GAFunctions2 import GAFunctions2
from heft.algs.hs.hs_scheme import run_hs, fit_converter
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.algs.ga.common_fixed_schedule_schema import generate as ga_generate
from heft.algs.common.utilities import unzip_result, data_to_file, gather_info, logbooks_in_data
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.common import AbstractExperiment
import time
from heft.core.vizualization.Vizualization import Vizualizator
import json
import os



class HSBaseExperiment(AbstractExperiment):

    @staticmethod
    def run(**kwargs):
        inst = HSBaseExperiment(**kwargs)
        return inst()

    def __init__(self):
        wf_name = "CyberShake_30"
        HS_PARAMS = {
            "n": 10,
            "gen_step": 10000,
            "is_silent": True
        }
        self.HS_PARAMS = HS_PARAMS
        self.wf_name = wf_name

    def __call__(self):
        _wf = wf(self.wf_name)
        rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
        # rm = ExperimentResourceManager(rg.r([30, 30, 20]))
        estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)

        empty_fixed_schedule_part = Schedule({node: [] for node in rm.get_nodes()})

        heft_schedule = run_heft(_wf, rm, estimator)

        #TODO make fixed schedule from HEFT
        #fixed_schedule = fix_schedule(empty_fixed_schedule_part, heft_schedule)
        fixed_schedule = empty_fixed_schedule_part

        ga_functions = GAFunctions2(_wf, rm, estimator)

        generate = partial(ga_generate, ga_functions=ga_functions,
                           fixed_schedule_part=fixed_schedule,
                           current_time=0.0, init_sched_percent=0.1,
                           initial_schedule=heft_schedule)

        stats = tools.Statistics(lambda ind: ind.fitness.values[0])
        stats.register("avg", numpy.mean)
        stats.register("std", numpy.std)
        stats.register("min", numpy.min)
        stats.register("max", numpy.max)

        logbook = tools.Logbook()
        logbook.header = ["gen", "evals"] + stats.fields

        toolbox = Toolbox()
        toolbox.register("generate", generate)
        toolbox.register("evaluate", fit_converter(ga_functions.build_fitness(empty_fixed_schedule_part, 0.0)))
        toolbox.register("clone", deepcopy)
        toolbox.register("mate", ga_functions.crossover)
        toolbox.register("sweep_mutation", ga_functions.sweep_mutation)
        toolbox.register("mutate", ga_functions.mutation)
        pop, logbook, best = run_hs(toolbox=toolbox,
                                logbook=logbook,
                                stats=stats,
                                **self.HS_PARAMS)

        resulted_schedule = ga_functions.build_schedule(best, empty_fixed_schedule_part, 0.0)

        #Vizualizator.create_jedule_visualization(resulted_schedule, "D:\Projects\heft_simulator\heft\heft\\vizual", "hs")

        Utility.validate_static_schedule(_wf, resulted_schedule)

        hs_makespan = Utility.makespan(resulted_schedule)
        return (hs_makespan, logbook)


def fix_schedule(res, heft):
    for item in heft.mapping:
        res.mapping[item] = res.mapping[item].append(heft.mapping[item])
    return res

if __name__ == "__main__":
    # start_time = time.clock()
    wf_list = ["Montage_25", "Montage_50", "Montage_75", "Montage_100",
               "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
               "Epigenomics_24", "Sipht_30", "Inspiral_30"]
    exp = HSBaseExperiment()
    for wf_name in wf_list:
        print(wf_name.upper())
        exp.wf_name = wf_name
        repeat_count = 50
        result, logbooks = unzip_result(repeat(exp, repeat_count))
        logbook = logbooks_in_data(logbooks)
        result_json = dict()
        result_json['result'] = round(numpy.mean(result), 1)
        result_json['iter'] = dict()
        for i in range(exp.HS_PARAMS['gen_step']):
            result_json['iter'][i] = dict()
            result_json['iter'][i]['min'] = round(logbook[(i, 'min')], 1)
            result_json['iter'][i]['time'] = round(logbook[(i, 'time')], 5)

        path = os.path.join("D:\papers\Polyrithm\experiments\HS", wf_name)
        if not os.path.exists(path):
            os.mkdir(path)
        outfile = open(path + "\\" + wf_name + ".txt", 'w')
        json.dump(result_json, outfile)
        outfile.close()
        print(result)
        print(numpy.mean(result))
        # print("Time = " + str(time.clock() - start_time))