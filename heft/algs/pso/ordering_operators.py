from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.common.particle_operations import MappingParticle, OrderingParticle, CompoundParticle
from heft.algs.ga.GAImplementation.GAFunctions2 import unmoveable_tasks
from heft.algs.pso.mapping_operators import construct_solution
from heft.algs.common.mapordschedule import ord_and_map, build_schedule as base_build_schedule, \
    validate_mapping_with_alive_nodes
from heft.algs.common.mapordschedule import fitness as basefitness
from heft.algs.pso.sdpso import velocity_update
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import Utility


def build_schedule(wf, rm, estimator, particle):
    ordering_particle = particle.ordering
    mapping_particle = particle.mapping
    ordering = numseq_to_ordering(wf, ordering_particle)
    solution = construct_solution(mapping_particle, ordering)
    sched = base_build_schedule(wf, estimator, rm, solution)
    return sched


def fitness(wf, rm, estimator, particle):
    sched = build_schedule(wf, rm, estimator, particle)
    return basefitness(wf, rm, estimator, sched)


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
        """
        this function is aimed to recover ordering
        taking into account parent-child relations
        :param ordering:
        :return:
        """

        # 1. for each taks you should verify can it be executed
        # regarding to parents have already been put to correct sequence

        left_ordering = [task_id for task_id, value in ordering]
        correct_ordering = [] + fixed_tasks_ids

        while len(left_ordering) > 0:
            print("---NEW_ITERATION---")

            length_before = len(correct_ordering)

            not_ready_to_execute = []
            for task_id in left_ordering:
                if wf.is_task_ready(task_id, correct_ordering):
                    correct_ordering.append(task_id)
                else:
                    not_ready_to_execute.append(task_id)
            left_ordering = not_ready_to_execute

            length_after = len(correct_ordering)

            assert length_before < length_after, 'there is no new task recovered for correct ordering. ' \
                                                 'length_before: {0}, length_after: {1}'.format(length_before, length_after) + ' ' \
                                                 'probably, error in the logic.'

        corrected_ordering = correct_ordering[len(fixed_tasks_ids):]
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


def ordering_update(w, c1, c2, p, best, pop, min=-1, max=1):
    new_velocity = velocity_update(w, c1, c2, p.best, best, p.velocity, p, pop)
    new_velocity.limit_by(min, max)
    new_entity = (p + new_velocity)
    new_entity.limit_by(min, max)
    p.entity = new_entity.entity
    p.velocity = new_velocity
    pass
