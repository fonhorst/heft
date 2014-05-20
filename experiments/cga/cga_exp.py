from functools import partial
from deap import tools
import distance
from scoop import futures
from GA.DEAPGA.coevolution.cga import Env, Specie, run_cooperative_ga, rounddeciter
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, mapping_default_mutate, mapping_default_initialize, ordering_default_crossover, ordering_default_mutate, ordering_default_initialize, ORDERING_SPECIE, default_choose, fitness_mapping_and_ordering, build_schedule
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.Utility import Utility
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf
from experiments.cga.utilities.common import UniqueNameSaver

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
selector = lambda env, pop: tools.selTournament(pop, len(pop), 4)

@rounddeciter
def hamming_distances(pop, ideal_ind):
    values = [distance.hamming(ideal_ind, p, normalized=True) for p in pop]
    #mn, mx = min(values), max(values)
    #values = [(v - mn)/mx for v in values]
    return values


def to_seq(mapping):
    srted = sorted(mapping, key=lambda x: x[0])
    return [n for t, n in srted]

def build_ms_ideal_ind():
    ## TODO: reconsider and make this procedure more stable
    res = []
    i = 0
    _nodes = sorted(rm.get_nodes(), key=lambda x: x.flops)
    for t in sorted(_wf.get_all_unique_tasks(), key=lambda x: x.id):
        res.append((t.id, _nodes[i].name))
        i = (i + 1 )%len(_nodes)
    return res

def build_os_ideal_ind():
    ## TODO: reconsider and make this procedure more stable
    return [t.id for t in sorted(_wf.get_all_unique_tasks(), key=lambda x: x.id)]

ms_ideal_ind = build_ms_ideal_ind()
os_ideal_ind = build_os_ideal_ind()

ms_str_repr = [{k: v} for k, v in ms_ideal_ind]


config = {
        "interact_individuals_count": 200,
        "generations": 300,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=50,
                           cxb=0.8, mb=0.5,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           mutate=mapping_default_mutate,
                           select=selector,
                           initialize=mapping_default_initialize,
                           stat=lambda pop: {"hamming_distances": hamming_distances([to_seq(p) for p in pop], to_seq(ms_ideal_ind))}
                           # stat=lambda pop: {"hamming_distances": "", "hamming_ideal_ind": ""}
                    ),
                    Specie(name=ORDERING_SPECIE, pop_size=50,
                           cxb=0.8, mb=0.5,
                           mate=ordering_default_crossover,
                           mutate=ordering_default_mutate,
                           select=selector,
                           initialize=ordering_default_initialize,
                           stat=lambda pop: {"hamming_distances": hamming_distances(pop, os_ideal_ind)}
                           # stat=lambda pop: {"hamming_distances": "", "hamming_ideal_ind": ""}
                    )
        ],

        "operators": {
            "choose": default_choose,
            "fitness": fitness_mapping_and_ordering
        }
    }


def do_experiment(saver, config):
    solution, pops, logbook = run_cooperative_ga(**config)
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
            }

        },
        "iterations": logbook
    }
    saver(data)
    return m

def repeat(func, n):
    fs = [futures.submit(func) for i in range(n)]
    futures.wait(fs)
    return [f.result() for f in fs]
    # return [func() for i in range(n)]


if __name__ == "__main__":
    saver = UniqueNameSaver("../../temp/cga_exp")
    res = repeat(partial(do_experiment, saver, config), 1)
    print("RESULTS: ")
    print(res)






