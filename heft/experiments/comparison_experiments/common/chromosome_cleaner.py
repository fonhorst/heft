from functools import reduce
import operator
import random

from heft.algs.common.particle_operations import CompoundParticle, MappingParticle, OrderingParticle
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import ScheduleItem


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

        not_scheduled_tasks = current_cleaned_schedule.get_unfailed_tasks_ids()

        working_nodes = [node.name for node in self._rm.get_nodes() if node.state != Node.Down]
        if len(working_nodes) == 0:
            raise ValueError("All nodes are down. This case can not be handled")

        for t in not_scheduled_tasks:
            if t in chromosome.mapping.entity:
                del chromosome.mapping.entity[t]

        ordering = {task_id: val for task_id, val in chromosome.ordering.entity.items()
                    if task_id not in not_scheduled_tasks}

        resolve_node = lambda node_name: working_nodes[0 if len(working_nodes) - 1 == 0 else random.randint(0, len(working_nodes) - 1)] \
            if node_name not in working_nodes else node_name

        chromosome.mapping.entity = {t: resolve_node(node_name) for t, node_name in chromosome.mapping.entity.items()}
        chromosome.mapping.velocity = MappingParticle.Velocity({})

        chromosome.ordering.entity = ordering
        chromosome.ordering.velocity = OrderingParticle.Velocity({})

        chromosome.best = None

        return chromosome
    pass



