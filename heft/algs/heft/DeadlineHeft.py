from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import Schedule, ScheduleItem
from heft.core.environment.Utility import timing
from heft.algs.heft.simple_heft import StaticHeftPlanner


class DeadlineHeft(StaticHeftPlanner):
    executed_tasks = set()

    def get_nodes(self):
        resources = self.resource_manager.get_resources()
        nodes = HeftHelper.to_nodes(resources)
        return nodes

    def __init__(self, workflow, resource_manager, estimator, ranking=None):
        self.current_schedule = Schedule(dict())
        self.workflow = workflow
        self.resource_manager = resource_manager
        self.estimator = estimator
        self.ranking = ranking

        self.current_time = 0

        nodes = self.get_nodes()
        pass

    @timing
    def run(self, current_cleaned_schedule):

        nodes = self.get_nodes()
        live_nodes = [node for node in nodes if node.state != Node.Down]

        for_planning = HeftHelper.get_tasks_for_planning(self.workflow, current_cleaned_schedule)
        for_planning = set([task.id for task in for_planning])

        self.wf_jobs = self.make_ranking(self.workflow, live_nodes) if self.ranking is None else self.ranking

        sorted_tasks = [task for task in self.wf_jobs if task.id in for_planning]

        # print("P: " + str(sorted_tasks))

        new_sched = self.mapping([(self.workflow, sorted_tasks)], current_cleaned_schedule.mapping, nodes, self.commcost, self.compcost)
        return new_sched

    def endtime(self, job, events):
        """ Endtime of job in list of events """
        for e in events:
            if e.job == job and (e.state == ScheduleItem.FINISHED or e.state == ScheduleItem.EXECUTING or e.state == ScheduleItem.UNSTARTED):
                return e.end_time

def run_heft(workflow, resource_manager, estimator):
    """
    It simply runs heft with empty initial schedule
    and returns complete schedule
    """
    heft = DeadlineHeft(workflow, resource_manager, estimator)
    nodes = resource_manager.get_nodes()
    init_schedule = Schedule({node: [] for node in nodes})
    return heft.run(init_schedule)



