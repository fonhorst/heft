from heft.algs.common.individuals import ListBasedIndividual
from heft.algs.ga.coevolution.cga import Env, Specie
from heft.algs.ga.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ORDERING_SPECIE, default_assign_credits, ordering_heft_based_initialize, overhead_fitness_mapping_and_ordering, default_build_solutions
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment import ResourceGenerator as rg
from heft.core.environment.Utility import wf
from heft.experiments.cga.cga_exp import hamming_distances, os_ideal_ind, ms_ideal_ind, do_experiment, hamming_for_best_components, best_components_itself, extract_mapping_from_ga_file, extract_ordering_from_ga_file, tourn
from heft.experiments.cga.utilities.common import UniqueNameSaver, repeat, build_os_ideal_ind
from heft.settings import __root_path__
_wf = wf("Montage_50")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=10)
selector = tourn
## TODO: remove this hack later
# class Fitness(ComparableMixin):
#     def __init__(self, fitness):
#         self.values = [fitness]
#
#     def _cmpkey(self):
#         return self.values[0]
#
#
# ## TODO: remake this stub later
# def roulette(env, pop):
#
#     for p in pop:
#         p.fitness = Fitness((1/-1*p.fitness)*100)
#
#     result = tools.selRoulette(pop, len(pop))
#
#     for p in pop:
#         p.fitness = (1/(p.fitness.values[0]/100)*-1)
#     return result
#
# selector = roulette

# def extract_mapping_from_file(path, wf, estimator, rm):
#     with open(path, 'r') as f:
#         data = json.load(f)
#     solution = data["final_solution"]
#     schedule = build_schedule(wf, estimator, rm, solution)
#     ind = [(item.job.id, node.name) for node, items in schedule.mapping.items() for item in items]
#     return ind
#
# def extract_ordering_from_file(path, wf, estimator, rm):
#     with open(path, 'r') as f:
#         data = json.load(f)
#     solution = data["final_solution"]
#     ordering = solution[ORDERING_SPECIE]
#     return ordering

# ms_representative = extract_mapping_from_file("../../temp/cga_exp_example/6685a2b2-78d6-4637-b099-ed91152464f5.json",
#                                               _wf, estimator, rm)

ms_representative = extract_mapping_from_ga_file("{0}/temp/ga_schedule_272 _tr100_m50.json".format(__root_path__), rm)

heft_ordering = extract_ordering_from_ga_file("{0}/temp/heft_etalon_full_tr100_m50.json".format(__root_path__))
heft_mapping = extract_mapping_from_ga_file("{0}/temp/heft_etalon_full_tr100_m50.json".format(__root_path__), rm)

os_ideal_ind = build_os_ideal_ind(_wf)
ms_ideal_ind = heft_mapping

config = {
        "interact_individuals_count": 200,
        "generations": 300,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, fixed=True,
                           representative_individual=ListBasedIndividual(ms_representative)),
                    Specie(name=ORDERING_SPECIE, pop_size=50,
                           cxb=0.8, mb=0.5,
                           mate=ordering_default_crossover,
                           mutate=ordering_default_mutate,
                           select=selector,
                           # initialize=ordering_default_initialize,
                           initialize=lambda ctx, pop: ordering_heft_based_initialize(ctx, pop, heft_ordering, 3),
                           stat=lambda pop: {"hamming_distances": hamming_distances(pop, os_ideal_ind)}
                    )
        ],
        "solstat": lambda sols: {"best_components": hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind),
                                 "best_components_itself": best_components_itself(sols)},
        "operators": {
            # "choose": default_choose,
            "build_solutions": default_build_solutions,
            # "fitness": fitness_mapping_and_ordering,
            "fitness": overhead_fitness_mapping_and_ordering,
            "assign_credits": default_assign_credits
        }
    }
saver = UniqueNameSaver("../../temp/cga_fixed_mapping")

def do_exp():
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)







