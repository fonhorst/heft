from datetime import datetime

from deap import tools

from GA.DEAPGA.coevolution.cga import Env, Specie, run_cooperative_ga
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ORDERING_SPECIE, build_schedule, max_assign_credits, mapping_heft_based_initialize, ordering_heft_based_initialize, fitness_mapping_and_ordering, MutRegulator, mapping_all_mutate_configurable, default_build_solutions, one_to_one_build_solutions
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.Utility import Utility
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf
from experiments.cga.utilities.common import UniqueNameSaver, repeat, tourn, ArchivedSelector, extract_mapping_from_ga_file, extract_ordering_from_ga_file, hamming_distances, to_seq, unique_individuals, pcm, gdm, hamming_for_best_components, best_components_itself


_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)


# selector = ArchivedSelector(5)(roulette)
mapping_selector = ArchivedSelector(3)(tourn)
ordering_selector = ArchivedSelector(3)(tourn)

# mapping_selector = tourn
# ordering_selector = tourn

# asel = ArchivedSelector(5)
# mapping_selector = asel(roulette)
# ordering_selector = ArchivedSelector(5)(roulette)


# heft_mapping = extract_mapping_from_file("../../temp/heft_etalon_tr100.json")
# heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")
# heft_ordering = extract_ordering_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")

heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json", rm)
heft_ordering = extract_ordering_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json")

ms_ideal_ind = heft_mapping
os_ideal_ind = heft_ordering

ms_str_repr = [{k: v} for k, v in ms_ideal_ind]

mapping_mut_reg = MutRegulator()


config = {
        "hall_of_fame_size": 0,
        "interact_individuals_count": 50,
        "generations": 100,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=50,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           # mutate=mapping_all_mutate,
                           # mutate=mapping_all_mutate_variable,
                           mutate=mapping_mut_reg(mapping_all_mutate_configurable),
                           # mutate=mapping_all_mutate_variable2,
                           # mutate=mapping_improving_mutation,
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
                           cxb=0.9, mb=0.9,
                           mate=ordering_default_crossover,
                           mutate=ordering_default_mutate,
                           select=ordering_selector,
                           # initialize=ordering_default_initialize,
                           initialize=lambda ctx, pop: ordering_heft_based_initialize(ctx, pop, heft_ordering, 3),
                           stat=lambda pop: {"hamming_distances": hamming_distances(pop, os_ideal_ind),
                                             "unique_inds_count": unique_individuals(pop),
                                             "pcm": pcm(pop),
                                             "gdm": gdm(pop)}
                    )
        ],
        "solstat": lambda sols: {"best_components": hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind),
                                 "best_components_itself": best_components_itself(sols),
                                 "best": -1*Utility.makespan(build_schedule(_wf, estimator, rm, max(sols, key=lambda x: x.fitness)))},

        "analyzers": [mapping_mut_reg.analyze],

        "operators": {
            # "choose": default_choose,
            # "build_solutions": default_build_solutions,
            "build_solutions": one_to_one_build_solutions,
            "fitness": fitness_mapping_and_ordering,
            # "fitness": overhead_fitness_mapping_and_ordering,
            # "assign_credits": default_assign_credits
            # "assign_credits": bonus2_assign_credits
            "assign_credits": max_assign_credits
        }
    }


def do_experiment(saver, config, _wf, rm, estimator):
    solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
    schedule = build_schedule(_wf, estimator, rm, solution)
    m = Utility.makespan(schedule)

    data = {
        "metainfo": {
            "interact_individuals_count": config["interact_individuals_count"],
            "generations": config["generations"],
            "species": [s.name for s in config["species"]],
            "pop_sizes": [s.pop_size for s in config["species"]],
            "nodes": {n.name: n.flops for n in config["env"].rm.get_nodes()},
            "ideal_inds": {
                MAPPING_SPECIE: ms_str_repr,
                ORDERING_SPECIE: os_ideal_ind
            },
            "wf_name": _wf.name
        },
        "initial_pops": initial_pops,
        "final_solution": solution,
        "final_makespan": m,
        "iterations": logbook
    }
    saver(data)
    return m



saver = UniqueNameSaver("../../temp/cga_exp")

def do_exp():
    ## TODO: remove time measure
    tstart = datetime.now()
    res = do_experiment(saver, config, _wf, rm, estimator)
    tend = datetime.now()
    tres = tend - tstart
    print("Time Result: " + str(tres.total_seconds()))
    return res
if __name__ == "__main__":

    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)






