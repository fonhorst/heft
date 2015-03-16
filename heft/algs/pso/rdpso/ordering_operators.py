from heft.algs.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from heft.algs.pso.rdpso.particle_operations import MappingParticle, OrderingParticle, CompoundParticle
from heft.algs.ga.GAImplementation.GAFunctions2 import unmoveable_tasks
from heft.algs.pso.rdpso.mapping_operators import construct_solution
from heft.algs.pso.rdpso.mapordschedule import ord_and_map, build_schedule as base_build_schedule, \
    validate_mapping_with_alive_nodes
from heft.algs.pso.rdpso.mapordschedule import fitness as basefitness
from heft.algs.pso.rdpso.rdpso import velocity_update, orderingTransform, mappingTransform
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import Utility
import copy


def build_schedule(wf, rm, estimator, particle, mapMatrix, rankList, filterList):
    ordering_particle = particle.ordering
    mapping_particle = copy.copy(particle.mapping)
    mapping_particle.entity = mappingTransform(mapping_particle.entity, mapMatrix, rm.get_nodes())
    ordering = numseq_to_ordering(wf, ordering_particle, rankList, filterList)
    solution = construct_solution(mapping_particle, ordering)
    sched = base_build_schedule(wf, estimator, rm, solution)
    return sched


def fitness(wf, rm, estimator, particle, mapMatrix, rankList, filterList):
    sched = build_schedule(wf, rm, estimator, particle, mapMatrix, rankList, filterList)
    return basefitness(wf, rm, estimator, sched)

#!!!!!!TODO ordering particle init, need create task rank list to ordering
def ordering_to_numseq(ordering, rankList):
    ord_position = []
    for job in ordering:
        ord_position.append((job, rankList[job]))
    return ord_position


def numseq_to_ordering(wf, ordering_particle, rankList, filterList, fixed_tasks_ids=[]):
    def recover_ordering(ordering):
        corrected_ordering = []

        while len(ordering) > 0:
            ord_iter = iter(ordering)
            while True:
                t = next(ord_iter)
                if Utility.is_enough_to_be_executed(wf, t, corrected_ordering + fixed_tasks_ids):
                    ordering.remove((t))
                    corrected_ordering.append(t)
                    break
                else:
                    #print("incorrect " + str([task[0] for task in ordering_particle.entity]))
                    pass
            pass
        return corrected_ordering

    ordering = orderingTransform(ordering_particle.entity, rankList, filterList)

    ordering = recover_ordering(ordering)

    for it in range(len(ordering_particle.entity)):
        ordering_particle.entity[it] = (ordering[it], ordering_particle.entity[it][1])

    return ordering

def test_valid(wf, ordering_particle, rankList, filterList, fixed_tasks_ids=[]):

    def recover_ordering(ordering):
        corrected_ordering = []

        for it in range(len(ordering)):
            t = ordering[it]
            if Utility.is_enough_to_be_executed(wf, t, corrected_ordering + fixed_tasks_ids):
                corrected_ordering.append(t)
            else:
                #print("INCORRECT")
                return False


    ordering = orderingTransform(ordering_particle.entity, rankList, filterList)
    #ordering = [task[0] for task in ordering_particle.entity]
    return recover_ordering(ordering)



def generate(wf, rm, estimator, mapMatrix=None, rankList=None, filterList=None, schedule=None, fixed_schedule_part=None, current_time=0.0):
    sched = schedule if schedule is not None else SimpleRandomizedHeuristic(wf, rm.get_nodes(), estimator).schedule(fixed_schedule_part, current_time)

    if fixed_schedule_part is not None:
        un_tasks = unmoveable_tasks(fixed_schedule_part)
        clean_sched = Schedule({node: [item for item in items if item.job.id not in un_tasks and item.state != ScheduleItem.FAILED]
                          for node, items in sched.mapping.items()})
    else:
        clean_sched = sched

    mapping, ordering = ord_and_map(clean_sched, mapMatrix)
    ordering_numseq = ordering_to_numseq(ordering, rankList)

    #ordering_map = {task_id: val for task_id, val in zip(ordering, ordering_numseq)}

    ord_p, map_p = OrderingParticle(ordering_numseq), MappingParticle(mapping)
    ord_p.velocity = OrderingParticle.Velocity({})
    #map_p.velocity = MappingParticle.Velocity({})
    result = CompoundParticle(map_p, ord_p)
    if schedule is None and not validate_mapping_with_alive_nodes(result.mapping.entity, rm, mapMatrix):
        raise Exception("found invalid solution in generated array")
    return result


def ordering_update(w, c1, c2, p, best, pop):
    new_velocity = velocity_update(w, c1, c2, p.best, best, p.velocity, p, pop)
    new_entity = (p + new_velocity)
    p.entity = new_entity.entity
    p.velocity = new_velocity
    pass
