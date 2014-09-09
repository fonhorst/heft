from functools import reduce
import operator
import random
from heft.algs.common.mapordschedule import check_precedence
from heft.algs.pso.ompso import CompoundParticle
from heft.algs.pso.sdpso import Particle
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

        ordering = [val for task_id, val in zip(self._wf.get_tasks_id(), chromosome.ordering.entity)
                    if task_id not in not_scheduled_tasks]


        # we don't need to clean the ordering, because of it isn't linked with failed nodes

        tasks_to_reschedule = [t for t, node in chromosome.mapping.entity.items() if node not in working_nodes]

        for t in tasks_to_reschedule:
            lt = len(working_nodes) - 1
            new_node = 0 if lt == 0 else random.randint(0, lt)
            node_name = working_nodes[new_node]
            chromosome.mapping.entity[t] = node_name

        chromosome.ordering.entity = ordering



        return chromosome
    pass


