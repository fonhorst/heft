from deap import tools
from scoop import futures
from GA.DEAPGA.coevolution.cga import Env, Specie, run_cooperative_ga
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, mapping_default_mutate, mapping_default_initialize, ordering_default_crossover, ordering_default_mutate, ordering_default_initialize, ORDERING_SPECIE, default_choose, fitness_mapping_and_ordering, build_schedule
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.Utility import Utility
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
selector = lambda env, pop: tools.selTournament(pop, len(pop), 4)
config = {
        "interact_individuals_count": 1000,
        "generations": 300,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=50,
                           cxb=0.8, mb=0.5,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           mutate=mapping_default_mutate,
                           select=selector,
                           initialize=mapping_default_initialize
                    ),
                    Specie(name=ORDERING_SPECIE, pop_size=50,
                           cxb=0.8, mb=0.5,
                           mate=ordering_default_crossover,
                           mutate=ordering_default_mutate,
                           select=selector,
                           initialize=ordering_default_initialize,
                    )
        ],

        "operators": {
            "choose": default_choose,
            "fitness": fitness_mapping_and_ordering
        }
    }


def experiment():
    solution = run_cooperative_ga(**config)
    schedule = build_schedule(_wf, estimator, rm, solution)
    m = Utility.makespan(schedule)
    return m

def repeat(func, n):
    fs = [futures.submit(func) for i in range(n)]
    futures.wait(fs)
    return [f.result() for f in fs]
    # return [func() for i in range(n)]


if __name__ == "__main__":
    res = repeat(experiment, 10)
    print("RESULTS: ")
    print(res)

