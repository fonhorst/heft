import random
from deap import tools
from GA.DEAPGA.coevolution.cga import Env, Specie, ListBasedIndividual
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE, default_choose, build_schedule, mapping_default_initialize, overhead_fitness_mapping_and_ordering, assign_from_transfer_overhead
from GA.DEAPGA.coevolution.utilities import build_ms_ideal_ind, build_os_ideal_ind, ArchivedSelector
from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from environment.ResourceGenerator import ResourceGenerator as rg
from environment.Utility import Utility
from experiments.cga import wf
from experiments.cga.cga_exp import repeat, hamming_distances, os_ideal_ind, ms_ideal_ind, do_experiment, unique_individuals, to_seq, hamming_for_best_components, best_components_itself, pcm, gdm, tourn, roulette
from experiments.cga.cga_fixed_ordering import extract_ordering_from_file
from experiments.cga.utilities.common import UniqueNameSaver

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)

# selector = ArchivedSelector(5)(tourn)
selector = ArchivedSelector(5)(roulette)


os_representative = extract_ordering_from_file("../../temp/cga_exp_example/6685a2b2-78d6-4637-b099-ed91152464f5.json",
                                              _wf, estimator, rm)

ms_ideal_ind = build_ms_ideal_ind(_wf, rm)
os_ideal_ind = build_os_ideal_ind(_wf)

# heft_mapping = extract_mapping_from_file("../../temp/heft_etalon_tr100.json")

saver = UniqueNameSaver("../../temp/cga_improving_mutation")

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

def do_exp():
    config = {
        "interact_individuals_count": 500,
        "generations": 1000,
        "env": Env(_wf, rm, estimator),
        "species": [Specie(name=MAPPING_SPECIE, pop_size=500,
                           cxb=0.9, mb=0.9,
                           mate=lambda env, child1, child2: tools.cxOnePoint(child1, child2),
                           # mutate=mapping_default_mutate,
                           # mutate=lambda ctx, mutant: mapping_k_mutate(ctx, 3, mutant)
                           # mutate=mapping_all_mutate,
                           mutate=mapping_improving_mutation,
                           select=selector,
                           initialize=mapping_default_initialize,
                           # initialize=lambda ctx, pop: mapping_heft_based_initialize(ctx, pop, heft_mapping, 3),
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
            "choose": default_choose,
            # "fitness": fitness_mapping_and_ordering,
            "fitness": overhead_fitness_mapping_and_ordering,
            # "assign_credits": default_assign_credits
            # "assign_credits": max_assign_credits
            "assign_credits": assign_from_transfer_overhead
        }
    }
    return do_experiment(saver, config, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 10)
    print("RESULTS: ")
    print(res)








