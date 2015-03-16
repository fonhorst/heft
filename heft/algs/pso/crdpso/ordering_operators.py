from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.pso.crdpso.particle_operations import MappingParticle, OrderingParticle, CompoundParticle
from heft.algs.ga.GAImplementation.GAFunctions2 import unmoveable_tasks
from heft.algs.pso.crdpso.mapping_operators import construct_solution
from heft.algs.pso.crdpso.mapordschedule import ord_and_map, build_schedule as base_build_schedule, \
    validate_mapping_with_alive_nodes
from heft.algs.pso.crdpso.mapordschedule import fitness as basefitness
from heft.algs.pso.crdpso.crdpso import velocity_update
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import Utility
from heft.algs.pso.crdpso.configuration_particle import make_rm
from copy import deepcopy


def build_schedule(wf, rm, estimator, particle):
    ordering_particle = particle.ordering
    mapping_particle = particle.mapping
    ordering = numseq_to_ordering(wf, ordering_particle)
    solution = construct_solution(mapping_particle, ordering)
    sched = base_build_schedule(wf, estimator, rm, solution)
    return sched


def fitness(wf, estimator, p1, p2):
    rm = make_rm(p2)
    sched = build_schedule(wf, rm, estimator, p1)
    return basefitness(wf, rm, estimator, sched)


def ordering_to_numseq(ordering, rank_list):
    ord_position = []
    for job in ordering:
        ord_position.append((job, rank_list[job]))
    return ord_position

def numseq_to_ordering(wf, ordering_particle, fixed_tasks_ids=[]):
    def recover_ordering(ordering):
        corrected_ordering = []

        while len(ordering) > 0:
            ord_iter = iter(ordering)
            while True:
                t, v = next(ord_iter)
                if Utility.is_enough_to_be_executed(wf, t, corrected_ordering + fixed_tasks_ids):
                    ordering.remove((t, v))
                    corrected_ordering.append(t)
                    break
                pass
            pass
        return corrected_ordering

    ordering = recover_ordering(deepcopy(ordering_particle.entity))
    return ordering


def generate(wf, rm, estimator, rank_list, schedule=None, fixed_schedule_part=None, current_time=0.0):
    sched = schedule if schedule is not None else SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule(fixed_schedule_part, current_time)

    if fixed_schedule_part is not None:
        un_tasks = unmoveable_tasks(fixed_schedule_part)
        clean_sched = Schedule({node: [item for item in items if item.job.id not in un_tasks and item.state != ScheduleItem.FAILED]
                          for node, items in sched.mapping.items()})
    else:
        clean_sched = sched

    mapping, ordering = ord_and_map(clean_sched)
    ordering = ordering_to_numseq(ordering, rank_list)

    ord_p, map_p = OrderingParticle(ordering), MappingParticle(mapping)
    ord_p.velocity = OrderingParticle.Velocity({})
    map_p.velocity = MappingParticle.Velocity({})
    result = CompoundParticle(map_p, ord_p)
    if schedule is None and not validate_mapping_with_alive_nodes(result.mapping.entity, rm):
        raise Exception("found invalid solution in generated array")
    return result


def ordering_update(w, c1, c2, p, best, rank_list):
    new_velocity = velocity_update(w, c1, c2, p.best, best, p.velocity, p)
    new_entity = (p + new_velocity).entity
    new_entity = ordering_transform(new_entity, rank_list)
    p.entity = new_entity
    p.velocity = new_velocity

# TODO optimize this terrible code
def ordering_transform(ordering, rank_list):
    res = []
    rank_copy = rank_list.copy()
    for it in range(len(ordering)):
        val = ordering[it]
        sub_list = [(task, abs(val[1] - rank)) for (task, rank) in rank_copy.items()]
        cur_task = min(sub_list, key=lambda t: t[1])[0]
        res.append((cur_task, val[1]))
        if cur_task != val[0]:
            swap_tasks(ordering, val[0], cur_task)
        del rank_copy[cur_task]
    return res

# TODO optimize this terrible code
def swap_tasks(ordering, t1, t2):
    fstList = [item[0] for item in ordering]
    t1Idx = fstList.index(t1)
    t2Idx = fstList.index(t2)
    ordering[t2Idx] = (t1, ordering[t2Idx][1])
    ordering[t1Idx] = (t2, ordering[t1Idx][1])

