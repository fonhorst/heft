import json
from deap import tools
from GA.DEAPGA.coevolution.cga import Env, Specie, ListBasedIndividual
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ordering_default_initialize, ORDERING_SPECIE, default_choose, fitness_mapping_and_ordering, default_assign_credits, build_schedule
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf
from experiments.cga.cga_exp import repeat, hamming_distances, os_ideal_ind, ms_ideal_ind, do_experiment, hamming_for_best_components, best_components_itself
from experiments.cga.utilities.common import UniqueNameSaver

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=10)
selector = lambda env, pop: tools.selTournament(pop, len(pop), 2)
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

def extract_mapping_from_file(path, wf, estimator, rm):
    with open(path, 'r') as f:
        data = json.load(f)
    solution = data["final_solution"]
    schedule = build_schedule(wf, estimator, rm, solution)
    ind = [(item.job.id, node.name) for node, items in schedule.mapping.items() for item in items]
    return ind

def extract_ordering_from_file(path, wf, estimator, rm):
    with open(path, 'r') as f:
        data = json.load(f)
    solution = data["final_solution"]
    ordering = solution[ORDERING_SPECIE]
    return ordering

ms_representative = extract_mapping_from_file("../../temp/cga_exp_example/6685a2b2-78d6-4637-b099-ed91152464f5.json",
                                              _wf, estimator, rm)

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
                           initialize=ordering_default_initialize,
                           stat=lambda pop: {"hamming_distances": hamming_distances(pop, os_ideal_ind)}
                    )
        ],
        "solstat": lambda sols: {"best_components": hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind),
                                 "best_components_itself": best_components_itself(sols)},
        "operators": {
            "choose": default_choose,
            "fitness": fitness_mapping_and_ordering,
            "assign_credits": default_assign_credits
        }
    }
saver = UniqueNameSaver("../../temp/cga_fixed_mapping")

def do_exp():
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 30)
    print("RESULTS: ")
    print(res)







