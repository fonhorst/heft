from functools import reduce
import operator
import random
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
        #old_size = reduce(operator.add, (len(tasks) for _, tasks in chromosome.items()), 0)

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

        #assert reduce(operator.add, (len(tasks) for _, tasks in chromosome.items()), 0) == old_size, "Chromosome size changed"

        return chromosome


class PSOChromosomeCleaner(ChromosomeCleaner):
    def __call__(self, chromosome, current_cleaned_schedule):
        raise NotImplementedError()
    pass


