from functools import reduce
import operator
import random
from heft.algs.common.individuals import DictBasedIndividual
from heft.algs.common.mapordschedule import check_precedence
from heft.algs.common.setbasedoperations import Position, Velocity
from heft.algs.pso.ompso import CompoundParticle, numseq_to_ordering
from heft.algs.pso.sdpso import MappingParticle
from heft.core.environment.BaseElements import Node

from heft.core.environment.ResourceManager import ScheduleItem
from heft.core.environment.Utility import Utility


class ChromosomeCleaner:
    def __call__(self, chromosome, current_cleaned_schedule):
        raise NotImplementedError()


class GaChromosomeCleaner(ChromosomeCleaner):
    def __init__(self, wf, rm, estimator):
        self._wf = wf
        self._rm = rm
        self._estimator = estimator
        pass

    def __call__(self, chromosome, current_cleaned_schedule):

        not_scheduled_tasks = [item.job.id for (node, items) in current_cleaned_schedule.mapping.items()
                               for item in items if item.state == ScheduleItem.FINISHED or
                               item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.UNSTARTED]

        working_nodes = [node.name for node in self._rm.get_nodes() if node.state != Node.Down]
        if len(working_nodes) == 0:
            raise ValueError("All nodes are down. This case can not be handled")

        for (node_name, ids) in chromosome.items():
            for_removing = []
            for id in ids:
                if id in not_scheduled_tasks:
                    for_removing.append(id)
                pass
            for r in for_removing:
                ids.remove(r)
                pass
            pass

        tasks_to_reschedule = reduce(operator.add, (tasks for node_name, tasks in chromosome.items()
                                                    if node_name not in working_nodes), [])

        for t in tasks_to_reschedule:
            lt = len(working_nodes) - 1
            new_node = 0 if lt == 0 else random.randint(0, lt)
            node_name = working_nodes[new_node]
            length = len(chromosome[node_name])
            # TODO: correct 0 and length
            new_place = 0 if length == 0 else random.randint(0, length)
            chromosome[node_name].insert(new_place, t)

        for node_name in chromosome:
            if node_name not in working_nodes:
                chromosome[node_name] = []

        return chromosome


class PSOChromosomeCleaner(ChromosomeCleaner):

    def __init__(self, wf, rm, estimator):
        self._wf = wf
        self._rm = rm
        self._estimator = estimator
        pass



    def __call__(self, chromosome, current_cleaned_schedule):
        if not isinstance(chromosome, CompoundParticle):
            raise ValueError("Chromosome is not of CompoundParticle type: {0}".format(type(chromosome)))

        not_scheduled_tasks = [item.job.id for (node, items) in current_cleaned_schedule.mapping.items()
                               for item in items if item.state == ScheduleItem.FINISHED or
                               item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.UNSTARTED]

        down_nodes = [node.name for node in self._rm.get_nodes() if node.state == Node.Down]
        print("PSO-cleaner Down_nodes: {0}".format(down_nodes))

        working_nodes = [node.name for node in self._rm.get_nodes() if node.state != Node.Down]
        if len(working_nodes) == 0:
            raise ValueError("All nodes are down. This case can not be handled")

        for t in not_scheduled_tasks:
            if t in chromosome.mapping.entity:
                del chromosome.mapping.entity[t]

        ## TODO: only for debug.  remove it later.
        val_ordering = [(task_id, val) for task_id, val in zip(self._wf.get_tasks_id(), chromosome.ordering.entity)
                        if task_id not in not_scheduled_tasks]
        if not check_precedence(self._wf, [task_id for task_id,_ in val_ordering]):
            raise Exception("Precedence violated!")

        ## TODO: bug lives here
        ordering = [val for task_id, val in zip(self._wf.get_tasks_id(), chromosome.ordering.entity)
                    if task_id not in not_scheduled_tasks]

        ordering = [val for task_id, val in zip(self._wf.get_tasks_id(), chromosome.ordering.entity)
                    if task_id not in not_scheduled_tasks]



        resolve_node = lambda node_name: working_nodes[0 if len(working_nodes) - 1 == 0 else random.randint(0, len(working_nodes) - 1)] \
            if node_name not in working_nodes else node_name

        fake_resolve_node = lambda node_name: working_nodes[0] #if node_name not in working_nodes else node_name
        tasks_to_reschedule = [t for t, node in chromosome.mapping.entity.items() if node not in working_nodes]


        # chromosome.mapping.entity = Position({t: resolve_node(node_name) for t, node_name in chromosome.mapping.entity.items()})
        chromosome.mapping.entity = Position({t: fake_resolve_node(node_name) for t, node_name in chromosome.mapping.entity.items()})
        chromosome.mapping.velocity = Velocity({})
        # new_entity = {t: resolve_node(node_name) for t, node_name in chromosome.mapping.entity.items()}

        #
        # for t in tasks_to_reschedule:
        #     lt = len(working_nodes) - 1
        #     new_node = 0 if lt == 0 else random.randint(0, lt)
        #     node_name = working_nodes[new_node]
        #     chromosome.mapping.entity[t] = node_name

        chromosome.ordering.entity = ordering
        chromosome.ordering.velocity = Velocity({})

        for task_id, node_name in chromosome.mapping.entity.items():
            if node_name not in working_nodes:
                raise Exception("Very bad")
        print("fixed tasks: {0}, mapping count: {1}".format(len(current_cleaned_schedule.get_unfailed_tasks_ids()),
                                                            len(chromosome.mapping.entity)))
        if len(chromosome.mapping.entity) + len(current_cleaned_schedule.get_unfailed_tasks_ids()) != len(self._wf.get_all_unique_tasks()):
            raise Exception("Not full chromosome")

        return chromosome
    pass

# class AlternativePSOChromosomeCleaner(GaChromosomeCleaner):
#
#     # TODO: merge with ParticleScheduleBuilder
#     def _particle_to_chromo(self, particle, current_cleaned_schedule):
#         """
#         Converts Particle representation of individual to chromosome representation used by GA operators
#         """
#         if isinstance(particle, CompoundParticle):
#             ordering = numseq_to_ordering(self._wf, particle, current_cleaned_schedule.get_unfailed_tasks_ids())
#             chromo_mapping = {node.name: [] for node in self._rm.get_nodes()}
#             for task_id in ordering:
#                 node_name = particle.mapping.entity[task_id]
#                 chromo_mapping[node_name].append(task_id)
#                 pass
#             chromo = DictBasedIndividual(chromo_mapping)
#             return chromo
#         raise ValueError("particle has a wrong type: {0}".format(type(particle)))
#
#     def _chromo_to_particle(self, chromo):
#         mapping = {task: node_name for node_name, tasks in chromo.items() for task in tasks}
#         ordering = {}
#
#         pass
#
#     def __call__(self, chromosome, current_cleaned_schedule):
#         chromo = self._particle_to_chromo(chromosome, current_cleaned_schedule)
#         result = super().__call__(chromo, current_cleaned_schedule)



