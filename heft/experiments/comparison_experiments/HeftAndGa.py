from copy import deepcopy
import os
import sys
from heft.settings import RESOURCES_PATH
import yaml
from heft.algs.common.utilities import unzip_result, logbooks_in_data, data_to_file
from heft.experiments.cga.utilities.common import repeat

from heft.algs.ga.GAImplementation.GAFunctions2 import mark_finished
from heft.algs.ga.GAImplementation.GAImpl import GAFactory
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.BladeExperimentalManager import TransferCalcExperimentEstimator, ExperimentEstimator
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import wf, Utility
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

import scoop
if scoop.IS_RUNNING:
    from scoop import futures
    map_func = futures.map
else:
    map_func = map

CURRENT_CONFIG = None
class Config(dict):
    @staticmethod
    def load_from_file(path):
        pathparts = path.split('\\')
        filename = pathparts[len(pathparts)-1]
        name = filename.split('.')[0]
        with open(path, "r") as f:
            cfg = yaml.load(f)

        return Config(cfg, name)

    def __init__(self, dct, name):
        super().__init__(dct)
        self.name = name
        self.ga_params = dct["ga_params"]
        self.wf_name = dct["wf_name"]

    # the dict have to contain the following params
    # - ga_params = {
    #         "Kbest": 5,
    #         "population": 100,
    #         "crossover_probability": 0.4, #0.3
    #         "replacing_mutation_probability": 0.2, #0.1
    #         "sweep_mutation_probability": 0.3, #0.3
    #         "generations": 25
    # }

    # - wf_name


class ParametrizedGaRunner:
    def __init__(self, config):
        self._config = config

        self._wf = wf(self._config.wf_name)
        # it's equal 10, 15, 25, 30 when ideal_flops == 1
        self._rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
        # estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
        #                                     ideal_flops=ideal_flops, transfer_time=100)
        self._estimator = ExperimentEstimator(ideal_flops=20, transfer_nodes=100, transfer_blades=100)
        # transfer_nodes means now channel bandwidth
        # MB_100_CHANNEL = 100*1024*1024
        # MB_100_CHANNEL = 7*1024*1024
        # estimator = TransferCalcExperimentEstimator(ideal_flops=ideal_flops,
        #                                             transfer_nodes=MB_100_CHANNEL, transfer_blades=100)
        pass

    def do_run_heft(self):
        heft_schedule = run_heft(self._wf, self._rm, self._estimator)
        # Utility.validate_static_schedule(_wf, heft_schedule)
        print("HEFT makespan: " + str(Utility.makespan(heft_schedule)))
        return heft_schedule

    def do_run_ga(self, initial_schedule):
        def default_fixed_schedule_part(resource_manager):
            fix_schedule_part = Schedule({node: [] for node in HeftHelper.to_nodes(resource_manager.get_resources())})
            return fix_schedule_part

        fix_schedule_part = default_fixed_schedule_part(self._rm)
        ((the_best_individual, pop, schedule, iter_stopped), logbook) = GAFactory.default()\
            .create_ga(silent=False,
                       wf=self._wf,
                       resource_manager=self._rm,
                       estimator=self._estimator,
                       ga_params=self._config.ga_params)(fix_schedule_part, initial_schedule)

        self._validate(self._wf, self._estimator, schedule)
        print("GA makespan: " + str(Utility.makespan(schedule)))
        return schedule, logbook

    # the func return the makespan of ga's schedule
    def __call__(self):
        heft_schedule = self.do_run_heft()
        ga_schedule, logbook = self.do_run_ga(initial_schedule=heft_schedule)
        return (Utility.makespan(ga_schedule), logbook)

    def _validate(self, _wf, estimator, schedule):
        sched = deepcopy(schedule)
        mark_finished(sched)
        Utility.validate_static_schedule(_wf, schedule)
        pass




if __name__ == '__main__':

    #if len(sys.argv) != 2:
    #    raise Exception("Path to config or folder with config is not found")

    # example of config is in resources folder: paramgarunner_example.yaml
    cfg_path = "E:\\Melnik\\heft\\resources\\config"
    if os.path.isdir(cfg_path):
        configs_to_be_executed = [os.path.join(cfg_path, el) for el in os.listdir(cfg_path)]
    else:
        configs_to_be_executed = [cfg_path]

    configs_to_be_executed = [Config.load_from_file(cfg) for cfg in configs_to_be_executed]

    repeat_count = 100
    for config in configs_to_be_executed:
        runner = ParametrizedGaRunner(config)
        result, logbooks = unzip_result(repeat(runner, repeat_count))
        logbook = logbooks_in_data(logbooks)
        data_to_file("./"+config.name+".txt", 300, logbook)
        print(result)