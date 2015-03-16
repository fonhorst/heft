from deap import tools
from algs.ga.coevolution.cga import Env, Specie, ListBasedIndividual
from algs.ga.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE, mapping2order_build_schedule, mapping_default_initialize, overhead_fitness_mapping_and_ordering, assign_from_transfer_overhead, mapping_all_mutate, default_build_solutions
from core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from core.environment.Utility import Utility
from core.environment.Utility import wf
from experiments.cga.cga_exp import os_ideal_ind, ms_ideal_ind, do_experiment
from experiments.cga.utilities.common import UniqueNameSaver, repeat, extract_ordering_from_file, ArchivedSelector, roulette, build_ms_ideal_ind, build_os_ideal_ind, hamming_distances, unique_individuals, to_seq, pcm, gdm, hamming_for_best_components, best_components_itself
from core.environment.ResourceGenerator import ResourceGenerator as rg
from config.settings import __root_path__

from heft.algs.common.individuals import ListBasedIndividual
from heft.algs.ga.coevolution.cga import Env, Specie
from heft.algs.ga.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE, build_schedule, mapping_default_initialize, overhead_fitness_mapping_and_ordering, assign_from_transfer_overhead, mapping_all_mutate, default_build_solutions
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.Utility import wf
from heft.experiments.cga.cga_exp import os_ideal_ind, ms_ideal_ind, do_experiment
from heft.experiments.cga.utilities.common import UniqueNameSaver, repeat, extract_ordering_from_file, ArchivedSelector, roulette, build_ms_ideal_ind, build_os_ideal_ind, hamming_distances, unique_individuals, to_seq, pcm, gdm, hamming_for_best_components, best_components_itself
from heft.settings import __root_path__
from heft.core.environment import Utility


_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)

# selector = ArchivedSelector(5)(tourn)
selector = ArchivedSelector(5)(roulette)


os_representative = extract_ordering_from_file("{0}/temp/cga_exp_example/6685a2b2-78d6-4637-b099-ed91152464f5.json".format(__root_path__),
                                              _wf, estimator, rm)

ms_ideal_ind = build_ms_ideal_ind(_wf, rm)
os_ideal_ind = build_os_ideal_ind(_wf)

# heft_mapping = extract_mapping_from_file("../../temp/heft_etalon_tr100.json")

saver = UniqueNameSaver("{0}/temp/cga_improving_mutation".format(__root_path__))


def do_exp():
    config = {
        "interact_individuals_count": 500,
        "generations": 1000,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=500,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           # mutate=mapping_default_mutate,
                           # mutate=lambda ctx, mutant: mapping_k_mutate(ctx, 3, mutant)
                           mutate=mapping_all_mutate,
                           # mutate=mapping_improving_mutation,
                           select=selector,
                           initialize=mapping_default_initialize,
                           # initialize=lambda ctx, pop: mapping_heft_based_initialize(ctx, pop, heft_mapping, 3),
                           stat=lambda pop: {"hamming_distances": hamming_distances([to_seq(p) for p in pop], to_seq(ms_ideal_ind)),
                                             "unique_inds_count": unique_individuals(pop),
                                             "pcm": pcm(pop),
                                             "gdm": gdm(pop)}

        ),
                    Specie(name=ORDERING_SPECIE, fixed=True,
                           representative_individual=ListBasedIndividual(os_representative))
        ],

        "solstat": lambda sols: {"best_components": hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind),
                                 "best_components_itself": best_components_itself(sols),
                                 #"best": -1*Utility.makespan(build_schedule(_wf, estimator, rm, max(sols, key=lambda x: x.fitness)))
                                 "best": -1*Utility.makespan(mapping2order_build_schedule(_wf, estimator, rm, max(sols, key=lambda x: x.fitness)))
                                 },

        "operators": {
            # "choose": default_choose,
            "build_solutions": default_build_solutions,
            # "fitness": fitness_mapping_and_ordering,
            "fitness": overhead_fitness_mapping_and_ordering,
            # "assign_credits": default_assign_credits
            # "assign_credits": max_assign_credits
            "assign_credits": assign_from_transfer_overhead
        }
    }
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)








