from functools import partial
from environment.ResourceManager import Scheduler
from environment.Resource import Node
from environment.Resource import SoftItem
from environment.ResourceManager import ScheduleItem
from environment.ResourceManager import Schedule
from environment.Utility import reverse_dict
from core.HeftHelper import HeftHelper


class StaticHeftPlanner(Scheduler):
    global_count = 0
    def __init__(self):
        self.task_rank_cache = dict()
        self.current_time = 0
        pass

    def compcost(self, job, agent):
        return self.estimator.estimate_runtime(job, agent)

    def commcost(self, ni, nj, A, B):
        return self.estimator.estimate_transfer_time(A, B, ni, nj)

    def make_ranking(self, wf, nodes):
        ##resources = self.resource_manager.get_resources()
        ##print("common nodes count:" + str(len(toNodes(resources))))
        ##nodes = HeftHelper.to_nodes(resources)
        ranking_func = HeftHelper.build_ranking_func(nodes, self.compcost, self.commcost)
        wf_jobs = ranking_func(wf)
        return wf_jobs


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
        nodes = HeftHelper.to_nodes(resources)

        wf_jobs = {wf: self.make_ranking(wf, nodes) for wf in sorted_wfs}

        ##new_schedule = self.get_unchanged_schedule(self.old_schedule, time)
        new_schedule = Schedule({node: [] for node in nodes})
        new_plan = new_schedule.mapping

        for (wf, jobs) in wf_jobs.items():
            new_schedule = self.mapping([(wf, jobs)],
                               new_plan,
                               nodes,
                               self.commcost,
                               self.compcost)
            new_plan = new_schedule.mapping

        return new_schedule

    def mapping(self, sorted_jobs, existing_plan, nodes, commcost, compcost):
        """def allocate(job, orders, jobson, prec, compcost, commcost):"""
        """ Allocate job to the machine with earliest finish time

        Operates in place
        """


        ## TODO: add finished tasks
        jobson = dict()
        for (node, items) in existing_plan.items():
            for item in items:
                if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.EXECUTING:
                    jobson[item.job] = node


        new_plan = existing_plan




        def ft(machine):
            #cost = st(machine)
            cost = st(machine) + compcost(task, machine)
            ##print("machine: %s job:%s cost: %s" % (machine.name, task.id, cost))
            return cost

        for wf, tasks in sorted_jobs:
            ##wf_dag = self.convert_to_parent_children_map(wf)
            wf_dag = HeftHelper.convert_to_parent_children_map(wf)
            prec = reverse_dict(wf_dag)
            for task in tasks:
                st = partial(self.start_time, wf, task, new_plan, jobson, prec, commcost)

                # ress = [(key, ft(key)) for key in new_plan.keys()]
                # agent_pair = min(ress, key=lambda x: x[1][0])
                # agent = agent_pair[0]
                # start = agent_pair[1][0]
                # end = agent_pair[1][1]

                agent = min(new_plan.keys(), key=ft)
                start = st(agent)
                end = ft(agent)

                new_plan[agent].append(ScheduleItem(task, start, end))
                jobson[task] = agent
        new_sched = Schedule(new_plan)
        return new_sched

    def start_time(self, wf, task, orders, jobson, prec, commcost, node):


        # def find_slots(node, comm_ready, runtime):
        #     node_schedule = orders.get(node, list())
        #     free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
        #     ## TODO: refactor it later
        #     f_time = max(free_time, comm_ready)
        #     base_variant = [(f_time, f_time + runtime)]
        #     zero_interval = [] if len(node_schedule) == 0 else [(0, node_schedule[0].start_time)]
        #     middle_intervals = [(node_schedule[i].end_time, node_schedule[i + 1].start_time) for i in range(len(node_schedule) - 1)]
        #     intervals = zero_interval + middle_intervals + base_variant
        #
        #     result = [(st, end) for (st, end) in intervals if st >= comm_ready and abs((end - st) - runtime) < 0.01]
        #     return result
        #
        # def comm_ready_func(task, node):
        #     ##TODO: remake this stub later.
        #     if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
        #         return 0
        #     return max([self.endtime(p, orders[jobson[p]]) + commcost(p, task, node, jobson[p]) for p in task.parents])
        #
        # def get_possible_execution_times(task, node):
        #     ## pay attention to the last element in the resulted seq
        #     ## it represents all available time of node after it completes all its work
        #     ## (if such interval can exist)
        #     ## time_slots = [(st1, end1),(st2, end2,...,(st_last, st_last + runtime)]
        #     runtime = self.estimator.estimate_runtime(task, node)
        #     comm_ready = comm_ready_func(task, node)
        #     time_slots = find_slots(node, comm_ready, runtime)
        #     return time_slots, runtime

        ## check if soft satisfy requirements
        if self.can_be_executed(node, task):
            ## static or running virtual machine
            if node.state is not Node.Down:

                # time_slots, runtime = get_possible_execution_times(task, node)
                # time_slot = time_slots[0]
                # start_time = time_slot[0]
                # end_time = start_time + runtime

                agent_ready = orders[node][-1].end_time if orders[node] else 0
                if len(task.parents) == 1 and wf.head_task.id == list(task.parents)[0].id:
                    comm_ready = 0
                else:
                ##if task in prec:
                    comm_ready = max([self.endtime(p, orders[jobson[p]])
                                      + commcost(p, task, node, jobson[p]) for p in task.parents])
                ##else:
                ##    comm_ready = 0
                #return (start_time, end_time)
                return max(agent_ready, comm_ready, self.current_time)
            else:
                return 1000000
                #return (1000000, -1)
        else:
            return 1000000
            #return (1000000, -1)

    def can_be_executed(self, node, job):
        ## check it
        return (job.soft_reqs in node.soft) or (SoftItem.ANY_SOFT in node.soft)

    def endtime(self, job, events):
        """ Endtime of job in list of events """
        # for e in reverse(events):
        #     if e.job.id == job.id:
        #         return e.end_time

        for e in events:
            if e.job == job:
                return e.end_time
