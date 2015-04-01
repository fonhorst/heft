from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.gsa.vm_cgsa.particle_operations import MappingParticle, OrderingParticle, CompoundParticle
from heft.algs.ga.GAImplementation.GAFunctions2 import unmoveable_tasks
from heft.algs.gsa.vm_cgsa.mapping_operators import construct_solution
from heft.algs.gsa.vm_cgsa.mapordschedule import ord_and_map, validate_mapping_with_alive_nodes
from heft.algs.gsa.vm_cgsa.operators import merge_rms, build_schedule as base_build_schedule
from heft.algs.gsa.vm_cgsa.mapordschedule import fitness as basefitness
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import Utility


def build_schedule(wf, rm, estimator, fix_sched, current_time, particle, config):
    ordering_particle = particle.ordering
    mapping_particle = particle.mapping
    fix_ids = []
    for node, items in fix_sched.mapping.items():
        for item in items:
            fix_ids.append(item.job.id)
    ordering = numseq_to_ordering(wf, ordering_particle, fix_ids)
    solution = construct_solution(mapping_particle, ordering)
    sched = base_build_schedule(wf, estimator, rm, fix_sched, current_time, solution, config)
    return sched

def fitness(wf, estimator, rm, fix_sched, current_time, p1, p2):
    """
    p1 - mapping, ordering
    p2 - ResourceManager
    """
    sched = build_schedule(wf, rm, estimator, fix_sched, current_time, p1, p2)
    merged_rm = merge_rms(rm, p2.entity)
    return basefitness(wf, merged_rm, estimator, sched)


def ordering_to_numseq(ordering, min=-1, max=1):
    step = abs((max - min)/len(ordering))
    initial = min
    ord_position = []
    for job_id in ordering:
        initial += step
        ord_position.append(initial)
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

    ordering = sorted(ordering_particle.entity.items(), key=lambda x: x[1])

    ordering = recover_ordering(ordering)
    return ordering


def generate(wf, rm, estimator, schedule=None, fixed_schedule_part=None, current_time=0.0):
    sched = schedule if schedule is not None else SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule(fixed_schedule_part, current_time)

    if fixed_schedule_part is not None:
        un_tasks = unmoveable_tasks(fixed_schedule_part)
        clean_sched = Schedule({node: [item for item in items if item.job.id not in un_tasks and item.state != ScheduleItem.FAILED]
                          for node, items in sched.mapping.items()})
    else:
        clean_sched = sched

    mapping, ordering = ord_and_map(clean_sched)
    ordering_numseq = ordering_to_numseq(ordering)

    ordering_map = {task_id: val for task_id, val in zip(ordering, ordering_numseq)}

    ord_p, map_p = OrderingParticle(ordering_map), MappingParticle(mapping)
    ord_p.velocity = OrderingParticle.Velocity({})
    map_p.velocity = MappingParticle.Velocity({})
    result = CompoundParticle(map_p, ord_p)
    if schedule is None and not validate_mapping_with_alive_nodes(result.mapping.entity, rm):
        raise Exception("found invalid solution in generated array")
    return result


