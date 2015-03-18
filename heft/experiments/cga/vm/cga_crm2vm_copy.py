from datetime import datetime
from functools import partial
import uuid

from heft.algs.ga.coevolution.cga_copy import Env, Specie, vm_run_cooperative_ga
from heft.algs.ga.coevolution.operators_copy import RESOURCE_CONFIG_SPECIE, GA_SPECIE, \
    vm_resource_default_initialize, resource_conf_crossover, ga_default_initialize, ga_mutate, ga_crossover, \
    max_assign_credits, MutRegulator, resource_config_mutate, \
    one_to_one_vm_build_solutions, fitness_ga_and_vm
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.Utility import wf
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.cga.utilities.common import BasicFinalResultSaver, repeat, tourn, ArchivedSelector, \
    extract_mapping_from_ga_file, extract_ordering_from_ga_file


class Config:
    def __init__(self, input_wf_name):

        self.wf_name = input_wf_name
        self._wf = wf(self.wf_name)
        self.rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
        self.rm.setVMParameter(80, 30)
        self.estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)

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
                               # mutate=mapping_all_mutate,
                               # mutate=mapping_all_mutate_variable,
                               mutate=ga_mutate,
                               # mutate=mapping_all_mutate_variable2,
                               # mutate=mapping_improving_mutation,
                               # mutate=mapping_default_mutate,
                               # mutate=MappingArchiveMutate(),
                               select=self.mapping_selector,
                               # initialize=mapping_default_initialize,
                               initialize=ga_default_initialize,
                               #                       stat=lambda pop: {"hamming_distances": hamming_distances([to_seq(p) for p in pop], to_seq(ms_ideal_ind)),
                               #                                        "unique_inds_count": unique_individuals(pop),
                               #                                       "pcm": pcm(pop),
                               #                                      "gdm": gdm(pop)}

            ),
                        Specie(name=RESOURCE_CONFIG_SPECIE, pop_size=100,
                               cxb=0.9, mb=0.9,
                               mate=resource_conf_crossover,
                               mutate=resource_config_mutate,
                               select=self.ordering_selector,
                               # initialize=ordering_default_initialize,
                               initialize=vm_resource_default_initialize,
                               #                           stat=lambda pop: {"hamming_distances": hamming_distances(pop, os_ideal_ind),
                               #                                            "unique_inds_count": unique_individuals(pop),
                               #                                           "pcm": pcm(pop),
                               #                                          "gdm": gdm(pop)}
                        )
            ],
            # "solstat": lambda sols: {"best_components": hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind),
            #                          "best_components_itself": best_components_itself(sols),
            #                          "best": -1*Utility.makespan(mapping2order_build_schedule(_wf, estimator, rm, max(sols, key=lambda x: x.fitness)))},

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
   # schedule = ga2resources_build_schedule(_wf, estimator, rm, hall[len(hall)-1])
   # print(print_sched(schedule))
    print("====================Experiment finished========================")

    max_value = max(hall.keys)

    data = {
        "final_makespan": max_value,
        "vm_series": vm_series,
        "final_resources": hall.items[len(hall.items) - 1][RESOURCE_CONFIG_SPECIE]
    }

    saver(data, number, config)
    return max_value


# saver = UniqueNameSaver("../../temp/cga_exp")


def do_exp(params):

    ## TODO: remove time measure
    tstart = datetime.now()

    number = params[0]
    wf_name = params[1]

    basic_saver = BasicFinalResultSaver("../../den_temp/coev_ga_vm_"+wf_name+"_exp")

    configuration = Config(wf_name)
    config = configuration.config
    for s in config["species"]: s.select = ArchivedSelector(3)(tourn)

    res = do_experiment(basic_saver, config, number)
    tend = datetime.now()
    tres = tend - tstart
    print("Time Result: " + str(tres.total_seconds()))
    return res


if __name__ == "__main__":

    wf_names = [
                "Montage_25"]
    for wf_name in wf_names:
        number = uuid.uuid4()
        res = repeat(partial(do_exp, [number, wf_name]), 1)
        print("RESULTS: ")
        print(res)






