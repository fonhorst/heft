import json
from deap import tools
from GA.DEAPGA.coevolution.cga import Env, Specie, ListBasedIndividual
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ordering_default_initialize, ORDERING_SPECIE, default_choose, fitness_mapping_and_ordering, default_assign_credits, build_schedule, mapping_default_mutate, mapping_default_initialize, mapping_k_mutate, max_assign_credits, mapping_all_mutate
from GA.DEAPGA.coevolution.utilities import ArchivedSelector
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf
from experiments.cga.cga_exp import repeat, hamming_distances, os_ideal_ind, ms_ideal_ind, do_experiment, unique_individuals, to_seq, hamming_for_best_components, best_components_itself, pcm, gdm
from experiments.cga.utilities.common import UniqueNameSaver, ComparableMixin
import random

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
# selector = lambda env, pop: tools.selTournament(pop, len(pop), 2)
## TODO: remove this hack later
class Fitness(ComparableMixin):
    def __init__(self, fitness):
        self.values = [fitness]

    def _cmpkey(self):
        return self.values[0]


## TODO: remake this stub later
def roulette(pop, l):

    for p in pop:
        p.fitness = Fitness((1/-1*p.fitness)*100)

    result = tools.selRoulette(pop, l)

    for p in pop:
        p.fitness = (1/(p.fitness.values[0]/100)*-1)
    return result

selector = ArchivedSelector(5)(roulette)

def extract_ordering_from_file(path, wf, estimator, rm):
    with open(path, 'r') as f:
        data = json.load(f)
    solution = data["final_solution"]
    ordering = solution[ORDERING_SPECIE]
    return ordering

os_representative = extract_ordering_from_file("../../temp/cga_exp_example/6685a2b2-78d6-4637-b099-ed91152464f5.json",
                                              _wf, estimator, rm)

saver = UniqueNameSaver("../../temp/cga_fixed_ordering")

def do_exp():
    config = {
        "interact_individuals_count": 500,
        "generations": 500,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=500,
                           cxb=0.8, mb=0.5,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           # mutate=mapping_default_mutate,
                           # mutate=lambda ctx, mutant: mapping_k_mutate(ctx, 3, mutant)
                           mutate=mapping_all_mutate,
                           select=selector,
                           initialize=mapping_default_initialize,
                           stat=lambda pop: {"hamming_distances": hamming_distances([to_seq(p) for p in pop], to_seq(ms_ideal_ind)),
                                             "unique_inds_count": unique_individuals(pop),
                                             "pcm": pcm(pop),
                                             "gdm": gdm(pop)}

        ),
                    Specie(name=ORDERING_SPECIE, fixed=True,
                           representative_individual=ListBasedIndividual(os_representative))
        ],

        "solstat": lambda sols: {"best_components": hamming_for_best_components(sols),
                                 "best_components_itself": best_components_itself(sols)},

        "operators": {
            "choose": default_choose,
            "fitness": fitness_mapping_and_ordering,
            # "assign_credits": default_assign_credits
            "assign_credits": max_assign_credits
        }
    }
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 5)
    print("RESULTS: ")
    print(res)








