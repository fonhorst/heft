from datetime import datetime

from deap import tools
from deap import creator
from deap import base

from GA.DEAPGA.coevolution.cga import Env, Specie, run_cooperative_ga, equal_mo_fitness_distribution, ListBasedIndividual
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ORDERING_SPECIE, build_schedule, max_assign_credits, mapping_heft_based_initialize, ordering_heft_based_initialize, fitness_mapping_and_ordering, MutRegulator, mapping_all_mutate_configurable, default_build_solutions, one_to_one_build_solutions, mapping_all_mutate
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.Utility import Utility
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf
from experiments.cga.mobjective.utility import SimpleTimeCostEstimator, fitness_makespan_and_cost_map_ord
from experiments.cga.utilities.common import UniqueNameSaver, repeat, tourn, ArchivedSelector, extract_mapping_from_ga_file, extract_ordering_from_ga_file, hamming_distances, to_seq, unique_individuals, pcm, gdm, hamming_for_best_components, best_components_itself, FakeSaver


_wf = wf("Montage_50")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None, ideal_flops=20, transfer_time=100)
env = Env(_wf, rm, estimator)

# selector = lambda ctx, pop:  tools.selNSGA2(pop, len(pop))
# selector = lambda ctx, pop:  tools.selTournament(pop, len(pop), 2)
# selector = lambda ctx, pop:  tools.selTournamentDCD(pop, len(pop))
def selector(ctx, pop, size=None):
    size = len(pop) if size is None else size
    return tools.selNSGA2(pop, size)
    # return tools.selTournamentDCD(pop, size)


# mapping_selector = ArchivedSelector(3)(selector)
# ordering_selector = ArchivedSelector(3)(selector)
mapping_selector = selector
ordering_selector = selector

# mapping_selector = tourn
# ordering_selector = tourn

# asel = ArchivedSelector(5)
# mapping_selector = asel(roulette)
# ordering_selector = ArchivedSelector(5)(roulette)


# heft_mapping = extract_mapping_from_file("../../temp/heft_etalon_tr100.json")
# heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")
# heft_ordering = extract_ordering_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")

heft_mapping = extract_mapping_from_ga_file("../../../temp/heft_etalon_full_tr100_m50.json", rm)
heft_ordering = extract_ordering_from_ga_file("../../../temp/heft_etalon_full_tr100_m50.json")

ms_ideal_ind = heft_mapping
os_ideal_ind = heft_ordering


creator.create("FitnessMin", base.Fitness, weights=(-1.0, -1.0))
creator.create("Individual", ListBasedIndividual, typecode='d', fitness=creator.FitnessMin)


ms_str_repr = [{k: v} for k, v in ms_ideal_ind]

mapping_mut_reg = MutRegulator()

os_representative = extract_ordering_from_ga_file("../../../temp/ga_schedule_272 _tr100_m50.json")

config = {
        "hall_of_fame_size": 0,
        "interact_individuals_count": 100,
        "generations": 200,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=40,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           mutate=mapping_all_mutate,
                           select=mapping_selector,
                           initialize=lambda ctx, pop: [creator.Individual(el) for el in mapping_heft_based_initialize(ctx, pop, heft_mapping, 3)],
                           stat=lambda pop: {}

                    ),
                    # Specie(name=ORDERING_SPECIE, pop_size=40,
                    #        cxb=0.9, mb=0.9,
                    #        mate=ordering_default_crossover,
                    #        mutate=ordering_default_mutate,
                    #        select=ordering_selector,
                    #        initialize=lambda ctx, pop: [creator.Individual(el) for el in ordering_heft_based_initialize(ctx, pop, heft_ordering, 3)],
                    #        stat=lambda pop: {}
                    # )
                    Specie(name=ORDERING_SPECIE, fixed=True,
                           representative_individual=creator.Individual(os_representative))

        ],
        "solstat": lambda sols: {},

        "analyzers": [mapping_mut_reg.analyze],

        "operators": {
            "build_solutions": default_build_solutions,
            # "build_solutions": one_to_one_build_solutions,
            "fitness_distribution": equal_mo_fitness_distribution,
            "fitness": lambda ctx, x: creator.FitnessMin(fitness_makespan_and_cost_map_ord(ctx, x)),
            "assign_credits": max_assign_credits,
            "empty_fitness": lambda: creator.FitnessMin((float("inf"), float("inf")))
            # "empty_fitness": lambda: creator.FitnessMin((-1000000000.0, -1000000000.0))
        }
    }


def do_experiment(saver, config, _wf, rm, estimator):
    solution, pops, logbook, initial_pops, solutions = run_cooperative_ga(**config)
    schedule = build_schedule(_wf, estimator, rm, solution)
    m = Utility.makespan(schedule)


    solutions.sort(key=lambda x: x.fitness.values)

    import matplotlib.pyplot as plt
    import numpy

    front = numpy.array([ind.fitness.values for ind in solutions])

    plt.scatter(front[:,0], front[:,1], c="b")
    plt.axis("tight")
    plt.show()

    return m



# saver = UniqueNameSaver("../../temp/cga_exp")
saver = FakeSaver()

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






