__author__ = 'Николай'

__author__ = 'nikolay'

from functools import partial
from collections import namedtuple
from heft.util import reverse_dict
from environment.ResourceManager import Scheduler
from environment.Resource import Node
from environment.Resource import SoftItem
from environment.ResourceManager import ScheduleItem
from environment.ResourceManager import Schedule
from environment.Resource import UP_JOB
from environment.Resource import DOWN_JOB


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
            ##TODO: remake it later
           return 0 if wf.priority is None else wf.priority

        ##simple inter priority sorting
        sorted_wfs = sorted(self.workflows, key=byPriority)
        wf_jobs = {wf: [] for wf in sorted_wfs}
        resources = self.resource_manager.get_resources()

        def toNodes(resources):
            result = set()
            for resource in resources:
                result.update(resource.nodes)
            return result

        print("common nodes count:" + str(len(toNodes(resources))))

        def compcost(job, agent):
            return self.estimator.estimate_runtime(job, agent)


        def commcost(ni, nj, A, B):
            return self.estimator.estimate_transfer_time(A, B, ni, nj)

        nodes = toNodes(resources)

        ##without mapping
        for wf in sorted_wfs:
            wf_dag = self.convert_to_parent_children_map(wf)
            rank = partial(self.ranking, nodes=nodes, succ=wf_dag,
                                         compcost=compcost, commcost=commcost)
            jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
            jobs = list(jobs)
            jobs = sorted(jobs, key=rank)
            wf_jobs[wf] = list(reversed(jobs))

        new_plan = {node:[] for node in nodes}
        new_schedule = None
        for (wf, jobs) in wf_jobs.items():
            new_schedule = self.mapping([(wf, jobs)],
                               new_plan,
                               nodes,
                               commcost,
                               compcost)
            new_plan = new_schedule.mapping

        return new_schedule

    def ranking(self, ni, nodes, succ, compcost, commcost):



        """ Rank of job

        This code is designed to mirror the wikipedia entry.
        Please see that for details

        [1]. http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time
        """
        ##rank = partial(self.ranking, compcost=compcost, commcost=commcost,
        ##               succ=succ, nodes=nodes)
        w = partial(self.avr_compcost, compcost=compcost, nodes=nodes)
        c = partial(self.avr_commcost, nodes=nodes, commcost=commcost)

        def estimate(ni):
            result = self.task_rank_cache.get(ni,None)
            if result is not None:
                return result
            if ni in succ and succ[ni]:
                result = w(ni) + max(c(ni, nj) + estimate(nj) for nj in succ[ni])
            else:
                result = w(ni)
            self.task_rank_cache[ni] = result
            return result

        """print( "%s %s" % (ni, result))"""
        result = estimate(ni)
        return result

    def avr_compcost(self,ni, nodes, compcost):
        """ Average computation cost """
        return sum(compcost(ni, node) for node in nodes) / len(nodes)


    def avr_commcost(self,ni, nj, nodes, commcost):
        ##TODO: remake it later
        ##return 0
        """ Average communication cost """
        n = len(nodes)
        if n == 1:
            return 0
        npairs = n * (n - 1)
        """commcost(ni, nj, a1, a2)"""
        """sum( 0 for a1 in nodes for a2 in nodes if a1 != a2)"""
        sm = 0
        l = len(nodes)
        a1 = 0

        """while a1 < l:
            a2 = 0
            while a2 < l:
                a2 += 1
                ##sm += 0##commcost(ni, nj, a1, a2)
            a1 += 1"""
        for a1 in range(0,l):
            for a2 in range(0,l):
               StaticHeftPlanner.global_count += 1
        """for a1 in nodes:
            for a2 in nodes:
                sm += 0"""
        return 1. * sm/npairs

    def convert_to_parent_children_map(self, wf):
        head = wf.head_task
        map = dict()
        def mapp(parents, map):
            for parent in parents:
                st = map.get(parent, set())
                st.update(parent.children)
                map[parent] = st
                mapp(parent.children, map)
        mapp(head.children, map)
        return map

    def mapping(self, sorted_jobs, existing_plan, nodes, commcost, compcost):
        """def allocate(job, orders, jobson, prec, compcost, commcost):"""
        """ Allocate job to the machine with earliest finish time

        Operates in place
        """

        jobson = dict()

        ##nodes = set()
        ##for resource in resources:
        ##    nodes.update(resource.nodes)

        new_plan = existing_plan


        def ft(machine):
            cost = st(machine) + compcost(task, machine)
            print("machine: %s job:%s cost: %s" % (machine.name, task.id, cost))
            return cost

        for wf, tasks in sorted_jobs:
            wf_dag = self.convert_to_parent_children_map(wf)
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
        """
            (We work without any window)
            1. check for correspondence of resource and job
            2. if resource is running(the last block in the old schedule is just a run), find first possible free time and put entry there
                - else if resource is up, find first possible free time and put entry there (it should be performed to find a better solution)
                - else if resource is down, add up-block and put entry next (it should be performed to find a better solution)
            3. if resource is ghost and there is a free slot, take the free slot and up the ghost,
            4. if resource is ghost, find first time of freeing a slot, take it and schedule uping of the ghost
        """

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
            """TODO: remake it later. It means task cannot be executed on this resource"""
            return 1000000

    def can_be_executed(self, node, job):
        ## check it
        return (job.soft_reqs in node.soft) or (SoftItem.ANY_SOFT in node.soft)

    def endtime(self, job, events):
        """ Endtime of job in list of events """
        for e in events:
            if e.job == job:
                return e.end_time
