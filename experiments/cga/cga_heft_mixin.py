from deap import tools
from algs.ga.coevolution.cga import Env, Specie
from algs.ga.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ordering_default_initialize, ORDERING_SPECIE, build_schedule, mapping_all_mutate, mapping_heft_based_initialize, overhead_fitness_mapping_and_ordering, assign_from_transfer_overhead, default_build_solutions
from core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from core.environment import Utility
from experiments.cga import wf
from experiments.cga.cga_exp import tourn, hamming_distances, unique_individuals, to_seq, pcm, gdm, hamming_for_best_components, best_components_itself, do_experiment
from experiments.cga.cga_fixed_ordering import extract_mapping_from_ga_file
from experiments.cga.utilities.common import UniqueNameSaver, repeat, ArchivedSelector, build_ms_ideal_ind, build_os_ideal_ind

_wf = wf("Montage_50")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
#
mapping_selector = ArchivedSelector(5)(tourn)
ordering_selector = ArchivedSelector(5)(tourn)

# mapping_selector = ArchivedSelector(5)(roulette)
# ordering_selector = ArchivedSelector(5)(roulette)


# heft_mapping = extract_mapping_from_file("../../temp/heft_etalon_tr100.json")
heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_tr100_m50.json")

ms_ideal_ind = build_ms_ideal_ind(_wf, rm)
os_ideal_ind = build_os_ideal_ind(_wf)


ms_str_repr = [{k: v} for k, v in ms_ideal_ind]


config = {
        "interact_individuals_count": 200,
        "generations": 10000,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=50,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           mutate=mapping_all_mutate,
                           # mutate=mapping_default_mutate,
                           # mutate=MappingArchiveMutate(),
                           select=mapping_selector,
                           # initialize=mapping_default_initialize,
                           initialize=lambda ctx, pop: mapping_heft_based_initialize(ctx, pop, heft_mapping, 3),
                           stat=lambda pop: {"hamming_distances": hamming_distances([to_seq(p) for p in pop], to_seq(ms_ideal_ind)),
                                             "unique_inds_count": unique_individuals(pop),
                                             "pcm": pcm(pop),
                                             "gdm": gdm(pop)}

                    ),
                    Specie(name=ORDERING_SPECIE, pop_size=50,
                           cxb=0.8, mb=0.5,
                           mate=ordering_default_crossover,
                           mutate=ordering_default_mutate,
                           select=ordering_selector,
                           initialize=ordering_default_initialize,
                           stat=lambda pop: {"hamming_distances": hamming_distances(pop, os_ideal_ind),
                                             "unique_inds_count": unique_individuals(pop),
                                             "pcm": pcm(pop),
                                             "gdm": gdm(pop)}
                    )
        ],
        "solstat": lambda sols: {"best_components": hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind),
                                 "best_components_itself": best_components_itself(sols),
                                 "best": -1*Utility.makespan(build_schedule(_wf, estimator, rm, max(sols, key=lambda x: x.fitness)))
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

saver = UniqueNameSaver("../../temp/cga_heft_mixin")

def do_exp():
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":

    res = repeat(do_exp, 5)
    print("RESULTS: ")
    print(res)






