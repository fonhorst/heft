from deap import tools
from algs.ga.coevolution.cga import Env, Specie, ListBasedIndividual
from algs.ga.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE, fitness_mapping_and_ordering, build_schedule, mapping_all_mutate, assign_from_transfer_overhead, mapping_heft_based_initialize, default_build_solutions
from core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from core.environment import Utility
from core.environment.Utility import wf
from experiments.cga.cga_exp import hamming_distances, os_ideal_ind, ms_ideal_ind, do_experiment, unique_individuals, to_seq, hamming_for_best_components, best_components_itself, pcm, gdm, tourn, \
    extract_mapping_from_ga_file, extract_ordering_from_ga_file
from experiments.cga.utilities.common import UniqueNameSaver, repeat, ArchivedSelector, build_ms_ideal_ind, build_os_ideal_ind

_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)

selector = ArchivedSelector(5)(tourn)

ms_ideal_ind = build_ms_ideal_ind(_wf, rm)
os_ideal_ind = build_os_ideal_ind(_wf)

# os_representative = extract_ordering_from_file("../../temp/cga_exp_example/6685a2b2-78d6-4637-b099-ed91152464f5.json",
#                                               _wf, estimator, rm)
# os_representative = extract_ordering_from_ga_file("../../temp/ga_schedule_272 _tr100_m50.json")
os_representative = extract_ordering_from_ga_file("../../temp/ga_schedule_full_439_tr100_m100.json")

# heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_tr100.json")
# heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")
heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json")

saver = UniqueNameSaver("../../temp/cga_fixed_ordering")

def do_exp():
    config = {
        "interact_individuals_count": 100,
        "generations": 300,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=50,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           # mutate=mapping_default_mutate,
                           # mutate=lambda ctx, mutant: mapping_k_mutate(ctx, 3, mutant)
                           mutate=mapping_all_mutate,
                           # mutate=OnlyUniqueMutant()(mapping_all_mutate),
                           select=selector,
                           # initialize=mapping_default_initialize,
                           initialize=lambda ctx, pop: mapping_heft_based_initialize(ctx, pop, heft_mapping, 3),
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
                                 "best": -1*Utility.makespan(build_schedule(_wf, estimator, rm, max(sols, key=lambda x: x.fitness)))
                                 },

        "operators": {
            # "choose": default_choose,
            "build_solutions": default_build_solutions,
            "fitness": fitness_mapping_and_ordering,
            # "fitness": overhead_fitness_mapping_and_ordering,
            # "assign_credits": default_assign_credits
            # "assign_credits": max_assign_credits
            "assign_credits": assign_from_transfer_overhead
        }
    }
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 3)
    print("RESULTS: ")
    print(res)








