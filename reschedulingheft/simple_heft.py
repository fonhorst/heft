from functools import partial
from environment.ResourceManager import Scheduler
from environment.Resource import Node
from environment.Resource import SoftItem
from environment.ResourceManager import ScheduleItem
from environment.ResourceManager import Schedule
from environment.Utility import reverse_dict
from reschedulingheft.HeftHelper import HeftHelper


class StaticHeftPlanner(Scheduler):
    global_count = 0
    def __init__(self):
        self.task_rank_cache = dict()
        pass

    def schedule(self):
        """
         create inter-priority
        """
        def byPriority(wf):
           return 0 if wf.priority is None else wf.priority

        ##simple inter priority sorting
        sorted_wfs = sorted(self.workflows, key=byPriority)
        wf_jobs = {wf: [] for wf in sorted_wfs}
        resources = self.resource_manager.get_resources()

        ##print("common nodes count:" + str(len(toNodes(resources))))

        def compcost(job, agent):
            return self.estimator.estimate_runtime(job, agent)


        def commcost(ni, nj, A, B):
            ##TODO: remake it later
            if A == B:
                return 0
            return 10
            ##return self.estimator.estimate_transfer_time(A, B, ni, nj)

        nodes = HeftHelper.to_nodes(resources)

        ranking_func = HeftHelper.build_ranking_func(nodes, compcost, commcost)

        wf_jobs = {wf: ranking_func(wf) for wf in sorted_wfs}

        ##new_schedule = self.get_unchanged_schedule(self.old_schedule, time)
        new_schedule = Schedule({node: [] for node in nodes})
        new_plan = new_schedule.mapping

        for (wf, jobs) in wf_jobs.items():
            new_schedule = self.mapping([(wf, jobs)],
                               new_plan,
                               nodes,
                               commcost,
                               compcost)
            new_plan = new_schedule.mapping

        return new_schedule

    def mapping(self, sorted_jobs, existing_plan, nodes, commcost, compcost):
        """def allocate(job, orders, jobson, prec, compcost, commcost):"""
        """ Allocate job to the machine with earliest finish time

        Operates in place
        """

        jobson = dict()


        new_plan = existing_plan


        def ft(machine):
            cost = st(machine) + compcost(task, machine)
            ##print("machine: %s job:%s cost: %s" % (machine.name, task.id, cost))
            return cost

        for wf, tasks in sorted_jobs:
            ##wf_dag = self.convert_to_parent_children_map(wf)
            wf_dag = HeftHelper.convert_to_parent_children_map(wf)
            prec = reverse_dict(wf_dag)
            for task in tasks:
                st = partial(self.start_time, task, new_plan, jobson, prec, commcost)

                agent = min(new_plan.keys(), key=ft)

                start = st(agent)
                end = ft(agent)

                new_plan[agent].append(ScheduleItem(task, start, end))
                jobson[task] = agent
        new_sched = Schedule(new_plan)
        return new_sched

    def start_time(self, task, orders, jobson, prec, commcost, node):

        ## check if soft satisfy requirements
        if self.can_be_executed(node, task):
            ## static or running virtual machine
            if node.state is not Node.Down:
                agent_ready = orders[node][-1].end_time if orders[node] else 0
                if task in prec:
                    comm_ready = max([self.endtime(p, orders[jobson[p]])
                                      + commcost(p, task, node, jobson[p]) for p in prec[task]])
                else:
                    comm_ready = 0
                return max(agent_ready, comm_ready)
        else:
            return 1000000

    def can_be_executed(self, node, job):
        ## check it
        return (job.soft_reqs in node.soft) or (SoftItem.ANY_SOFT in node.soft)

    def endtime(self, job, events):
        """ Endtime of job in list of events """
        for e in events:
            if e.job == job:
                return e.end_time
