from datetime import datetime
from functools import partial
import uuid

from heft.algs.ga.coevolution.cga import Env, Specie, vm_run_cooperative_ga
from heft.algs.ga.coevolution.operators import RESOURCE_CONFIG_SPECIE, GA_SPECIE, \
    vm_resource_default_initialize, resource_conf_crossover, ga_default_initialize, ga_mutate, ga_crossover, \
    max_assign_credits, MutRegulator, resource_config_mutate, \
    one_to_one_vm_build_solutions, fitness_ga_and_vm
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.ResourceManager import ScheduleItem, Schedule
from heft.core.environment.Utility import wf
from heft.core.environment.BladeResourceGenrator import ResourceGenerator as rg
from heft.experiments.cga.utilities.common import BasicFinalResultSaver, repeat, tourn, ArchivedSelector, \
    extract_mapping_from_ga_file, extract_ordering_from_ga_file
from heft.algs.common.utilities import unzip_result
from heft.algs.heft.DSimpleHeft import run_heft
import numpy
import os


class Config:
    def __init__(self, input_wf_name):

        self.wf_name = input_wf_name
        self._wf = wf(self.wf_name)
        # input changed from 1 dimension list to 2 dimensions
        self.rm = ExperimentResourceManager(rg.generate_resources([[15, 15, 20, 30]]))
        self.rm.resources[0].nodes[0].state = "down"
        self.rm.setVMParameter([(80, 30)])
        # now transfer time less, if nodes from one blade
        self.estimator = ExperimentEstimator(ideal_flops=20, transfer_nodes=100, transfer_blades=100)

        self.mapping_selector = ArchivedSelector(3)(tourn)
        self.ordering_selector = ArchivedSelector(3)(tourn)

        self.mapping_mut_reg = MutRegulator()

        self.config = {
            "hall_of_fame_size": 5,
            "interact_individuals_count": 100,
            "generations": 10,
            "env": Env(self._wf, self.rm, self.estimator),
            "species": [Specie(name=GA_SPECIE, pop_size=50,
                               cxb=0.5, mb=0.5,
                               mate=ga_crossover,
                               mutate=ga_mutate,
                               select=self.mapping_selector,
                               initialize=ga_default_initialize,
                               ),
                        Specie(name=RESOURCE_CONFIG_SPECIE, pop_size=50,
                               cxb=0.5, mb=0.5,
                               mate=resource_conf_crossover,
                               mutate=resource_config_mutate,
                               select=self.ordering_selector,
                               initialize=vm_resource_default_initialize,
                               )
                        ],

            "analyzers": [self.mapping_mut_reg.analyze],

            "operators": {
                "build_solutions": one_to_one_vm_build_solutions(),
                "fitness": fitness_ga_and_vm,
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

def resources_printer(resources):
    for res in resources:
        print([node.name + ":" + str(node.flops) for node in res])

def _get_fixed_schedule(schedule, front_event):
            def is_before_event(item):
                # hard to resolve corner case. The simulator doesn't guranteed the order of appearing events.
                if item.start_time < front_event.end_time:
                    return True
                if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.FAILED:
                    return True
                return False
            def set_proper_state(item):
                new_item = ScheduleItem.copy(item)
                non_finished = new_item.state == ScheduleItem.EXECUTING or new_item.state == ScheduleItem.UNSTARTED
                if non_finished and new_item.end_time <= front_event.end_time:
                    new_item.state = ScheduleItem.FINISHED
                if non_finished and new_item.end_time > front_event.end_time:
                    new_item.state = ScheduleItem.EXECUTING
                return new_item
            fixed_mapping = {key: [set_proper_state(item) for item in items if is_before_event(item)] for (key, items) in schedule.mapping.items()}
            return Schedule(fixed_mapping)

def do_experiment(saver, config, number):

    # create HEFT schedule
    heft_schedule = run_heft(config["env"][0], config["env"][1], config["env"][2])
    for node, sched in heft_schedule.mapping.items():
        if len(sched) > 0:
            first_event = sched[0]
            break
    fixed_schedule = _get_fixed_schedule(heft_schedule, first_event)
    config["fixed_schedule"] = fixed_schedule
    config["initial_schedule"] = heft_schedule
    config["current_time"] = 0
    config["initial_population"] = None

    solution, pops, logbook, initial_pops, hall, vm_series = vm_run_cooperative_ga(**config)
    #print("====================Experiment finished========================")

    if len(hall.keys) == 0:
       print("We have a problem, officer")
    #print(hall.keys)
    max_value = max(hall.keys)
    #print("Solution's resources: ")
    #resources_printer(solution["ResourceConfigSpecie"])
    # TODO now doesn't need
    """
    data = {
        "final_makespan": max_value,
        "vm_series": vm_series,
        "final_resources": hall.items[len(hall.items) - 1][RESOURCE_CONFIG_SPECIE]
    }
    saver(data, number, config)
    """

    # Convert logbook to list of best for each generation
    res_logbook = logbook_to_bests(logbook)
    return max_value, res_logbook

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
                "Montage_25", "Montage_50", "Montage_75", "Montage_100",
                "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100"
                ]
    wf_names = ["Montage_25"]
    dir = "./cga_results/"
    repeat_count = 1

    for wf_name in wf_names:
        print("++++++========++++++++")
        print(wf_name.upper())
        print("")
        number = uuid.uuid4()

        result = repeat(partial(do_exp, [number, wf_name]), repeat_count)
        print("FINISH")
        res, logbooks = unzip_result(result)

        # Output to file
        res_log = logbooks_reduce(logbooks)
        logs_to_file(res_log, dir, wf_name)

        print("RESULTS: ")
        print(res)
        print('mean = )')
        print(-numpy.mean(res))
        print("")






