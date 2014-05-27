from copy import deepcopy
from datetime import datetime
import json
import random
import math

from deap import tools
import distance

from GA.DEAPGA.coevolution.cga import Env, Specie, run_cooperative_ga, rounddeciter, ListBasedIndividual
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ordering_default_crossover, ordering_default_mutate, ordering_default_initialize, ORDERING_SPECIE, default_choose, build_schedule, max_assign_credits, mapping_all_mutate, overhead_fitness_mapping_and_ordering, \
    mapping_heft_based_initialize, ordering_heft_based_initialize, default_assign_credits, fitness_mapping_and_ordering, mapping_all_mutate_variable, \
    mapping_default_initialize, mapping_all_mutate_variable2
from GA.DEAPGA.coevolution.utilities import build_ms_ideal_ind, build_os_ideal_ind, ArchivedSelector
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.Utility import Utility
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga import wf
from experiments.cga.utilities.common import UniqueNameSaver, ComparableMixin, repeat


_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
tourn = lambda ctx, pop: tools.selTournament(pop, len(pop), 2)
## TODO: remove this hack later
class Fitness(ComparableMixin):
    def __init__(self, fitness):
        self.values = [fitness]

    def _cmpkey(self):
        return self.values[0]


## TODO: remake this stub later
def roulette(ctx, pop):

    for p in pop:
        p.fitness = Fitness((1/-1*p.fitness)*100)

    result = tools.selRoulette(pop, len(pop))

    for p in pop:
        p.fitness = (1/(p.fitness.values[0]/100)*-1)
    return result


def mapping_improving_mutation(ctx, mutant):
    env = ctx["env"]
    task_to_node = {t: n for t, n in mutant}

    def estimate_overheads(task_id, node_name):
        task = env.wf.byId(task_id)
        node = env.rm.byName(node_name)
        ttime = env.estimator.estimate_transfer_time
        ptransfer_time = [ttime(node, env.rm.byName(task_to_node[p.id]), task, p) for p in task.parents if p != env.wf.head_task]
        ctransfer_time = [ttime(node, env.rm.byName(task_to_node[p.id]), task, p) for p in task.children]
        computation_time = env.estimator.estimate_runtime(task, node)
        return (ptransfer_time, ctransfer_time, computation_time)

    overheads = {t: estimate_overheads(t, n) for t,n in task_to_node.items()}
    sorted_overheads = sorted(overheads.items(), key=lambda x: x[1][0] + x[1][1])

    ## choose overhead for improving
    # try to improve max transfer overhead
    # t, oheads = sorted_overheads[0]
    for i in range(50):
        t, oheads = sorted_overheads[random.randint(0, int(len(sorted_overheads)/2))]

        # improving
        nodes = env.rm.get_nodes()
        potential_overheads = [(n, estimate_overheads(t, n.name)) for n in nodes if task_to_node[t] != n.name]

        n, noheads = min(potential_overheads, key=lambda x: x[1][0] + x[1][1])
        if oheads[0] + oheads[1] > noheads[0] + noheads[1]:
            for i in range(len(mutant)):
                t1, n1 = mutant[i]
                if t1 == t:
                    mutant[i] = (t, n.name)
                    break
                pass
    pass

# selector = ArchivedSelector(5)(roulette)
mapping_selector = ArchivedSelector(3)(tourn)
ordering_selector = ArchivedSelector(3)(tourn)

# mapping_selector = tourn
# ordering_selector = tourn

# asel = ArchivedSelector(5)
# mapping_selector = asel(roulette)
# ordering_selector = ArchivedSelector(5)(roulette)

@rounddeciter
def hamming_distances(pop, ideal_ind):
    values = [distance.hamming(ideal_ind, p, normalized=True) for p in pop]
    #mn, mx = min(values), max(values)
    #values = [(v - mn)/mx for v in values]
    return values


def to_seq(mapping):
    srted = sorted(mapping, key=lambda x: x[0])
    return [n for t, n in srted]




ms_ideal_ind = build_ms_ideal_ind(_wf, rm)
os_ideal_ind = build_os_ideal_ind(_wf)

def hamming_for_best_components(sols, ms_ideal_ind, os_ideal_ind):
    result = {s_name: hamming_distances([ind], ms_ideal_ind if s_name == MAPPING_SPECIE else os_ideal_ind)[0]
              for s_name, ind in max(sols, key=lambda x: x.fitness).items()}
    return result

def unique_individuals(pop):
    unique_hashes = set(hash(tuple(p)) for p in pop)
    return len(unique_hashes)

def best_components_itself(sols):
    result = {s_name: deepcopy(ind)
              for s_name, ind in max(sols, key=lambda x: x.fitness).items()}
    return result

## measure of phenotype convergence
## as it is normalized, pdm = 1 - pcm
## pdm == phenotype diversity measure
## it means pcm == 1 - we are in a local minimum
def pcm(pop):
    mx = max(pop, key=lambda x: x.fitness).fitness
    mn = min(pop, key=lambda x: x.fitness).fitness

    ## loglp(x) = ln(1 + x)
    sm = lambda arr: sum(math.log1p(math.fabs(p1 - p2)) for p1, p2 in zip(arr[1:len(arr) - 1], arr[2:]))
    numerator = sm(sorted([p.fitness for p in pop]))

    if mx != mn:
        uniform_dist = math.fabs(mx - mn)/(len(pop) - 1)

        i = mn
        arr = []
        while i <= mx:
            arr.append(i)
            i += uniform_dist

        vmd = sm(arr)
        measure = 1 - numerator/vmd
    else:
        measure = 1
    return measure

## genotype diversity measure
## 0 - divesity absence,
## 1 - great diversity
def gdm(pop):
    s1 = 0
    for i in range(len(pop) - 1):
        s2 = 0
        for j in range(i + 1, len(pop)):
            s2 += distance.hamming(pop[i], pop[j], normalized=True)
        s1 += (s2/(len(pop) - i))
    measure = s1 / len(pop)
    return measure


def extract_ordering_from_file(path, wf, estimator, rm):
    with open(path, 'r') as f:
        data = json.load(f)
    solution = data["final_solution"]
    ordering = solution[ORDERING_SPECIE]
    return ordering

def extract_mapping_from_ga_file(path):
    with open(path, 'r') as f:
        data = json.load(f)
    ## TODO: this is pure hack. It is needed to be refactored or removed
    nodes = {node.flops: node.name for node in rm.get_nodes()}

    mapping = ListBasedIndividual([(t, nodes[n]) for t, n in data["mapping"]])
    return mapping

def extract_ordering_from_ga_file(path):
    with open(path, 'r') as f:
        data = json.load(f)

    ordering = ListBasedIndividual([t for t in data["ordering"]])
    return ordering

ms_str_repr = [{k: v} for k, v in ms_ideal_ind]


# heft_mapping = extract_mapping_from_file("../../temp/heft_etalon_tr100.json")
# heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")
# heft_ordering = extract_ordering_from_ga_file("../../temp/heft_etalon_full_tr100_m50.json")

heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json")
heft_ordering = extract_ordering_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json")


config = {
        "interact_individuals_count": 100,
        "generations": 1000,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=50,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           # mutate=mapping_all_mutate,
                           # mutate=mapping_all_mutate_variable,
                           mutate=mapping_all_mutate_variable2,
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
                                 "best_components_itself": best_components_itself(sols)},
        "operators": {
            "choose": default_choose,
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



saver = UniqueNameSaver("../../temp/cga_exp_2")

def do_exp():
    ## TODO: remove time measure
    tstart = datetime.now()
    res = do_experiment(saver, config, _wf, rm, estimator)
    tend = datetime.now()
    tres = tend - tstart
    print("Time Result: " + str(tres.total_seconds()))
    return res
if __name__ == "__main__":

    res = repeat(do_exp, 10)
    print("RESULTS: ")
    print(res)






