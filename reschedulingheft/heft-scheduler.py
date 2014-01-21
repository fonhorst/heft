from functools import partial
from environment.Utility import reverse_dict
from environment.ResourceManager import Scheduler
from environment.Resource import Node
from environment.Resource import SoftItem
from environment.ResourceManager import ScheduleItem
from environment.ResourceManager import Schedule
from environment.Resource import UP_JOB
from environment.Resource import DOWN_JOB
import math

##HEFT with management of virtual machine and full rescheduling in dynamic
from reschedulingheft.HeftHelper import HeftHelper


class ReschedulingHeftPlanner(Scheduler):
    def __init__(self, up_time, down_time, generate_new_ghost_machine):
        ##self.task_rank_cache = dict()
        self.up_time = up_time
        self.down_time = down_time
        self.generate_new_ghost_machine = generate_new_ghost_machine
        pass

    def schedule(self, time):

        """
         create inter-priority
        """

        def byPriority(wf):
           return 0 if wf.priority is None else wf.priority

        ##simple inter priority sorting
        sorted_wfs = sorted(self.workflows, key=byPriority)
        resources = self.resource_manager.get_resources()

        def compcost(job, agent):
            return self.estimator.estimate_runtime(job, agent)


        def commcost(ni, nj, A, B):
            return self.estimator.estimate_transfer_time(A, B, ni, nj)

        nodes = HeftHelper.to_nodes(resources)

        ranking_func = HeftHelper.build_ranking_func(nodes, compcost, commcost)

        wf_jobs = {wf: ranking_func(wf) for wf in sorted_wfs}

        new_schedule = self.get_unchanged_schedule(self.old_schedule, time)

        for jobs in wf_jobs:
            new_schedule = self.mapping(jobs,
                               resources,
                               new_schedule,
                               commcost,
                               compcost,
                               time,
                               self.up_time,
                               self.down_time,
                               self.generate_new_ghost_machine)

        return new_schedule



    def node_count_by_soft(self, ni, nodes):
        count = 0
        for node in nodes:
           count += 1 if self.can_be_executed(node, ni) else 0
        return count

    def get_unchanged_schedule(self, sched, time):
        new_plan = dict()
        for vm, plan in sched.plan.items():
            unstarted_events = list(filter(lambda evt: evt.start < time, plan))
            new_plan[vm] = unstarted_events
        new_sched = Schedule(id, new_plan)
        return new_sched
        ##return sched.get_schedule_in_time(time)

    def mapping(self, sorted_jobs, nodes, existing_schedule, commcost, compcost, time, up_time, down_time, generate_new_ghost_machine):
        """def allocate(job, orders, jobson, prec, compcost, commcost):"""
        """ Allocate job to the machine with earliest finish time

        Operates in place
        """

        jobson = dict()

        new_plan = existing_schedule.mapping

        def ft(machine):
            cost = st(machine) + compcost(task, machine)
            print("machine: %s job:%s cost: %s" % (machine.id, task.id, cost))
            return cost

        for wf, tasks in sorted_jobs:
            wf_dag = self.convert_to_parent_children_map(wf)
            prec = reverse_dict(wf_dag)
            for task in tasks:
                print("======================")
                st = partial(self.start_time, task, new_plan, jobson, prec, commcost, up_time, down_time, time)

                agent = min(new_plan.keys(), key=ft)

                start = st(agent)
                end = ft(agent)

                if start >= 1000000:
                    ghost = generate_new_ghost_machine()
                    new_plan[ghost] = []
                    ghost.soft = [SoftItem.ANY_SOFT]
                    print("Creating new ghost: %s cost: %s" % (ghost.id, ft(ghost)))
                    agent = ghost
                    start = st(agent)
                    end = ft(agent)


                if agent.state == Node.Down:
                    free_time, slot = self.take_free_slot_or_find_freepoint_of_busy(agent, new_plan, time)
                    """TODO: remake it later"""
                    start = max(start, self.register_vm_for_slot(free_time, slot, agent, new_plan, up_time, down_time, task, False))
                    end = start + compcost(task, agent)

                new_plan[agent].append(ScheduleItem(task, start, end))
                jobson[task] = agent

        new_sched = Schedule(new_plan)
        return new_sched

    def start_time(self, task, orders, jobson, prec, commcost, up_time, down_time, time, node):
        """
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
                ##find time and slot where running vm will become empty
                free_time, slot = self.take_free_slot_or_find_freepoint_of_busy(node, orders, time)
                real_start = self.register_vm_for_slot(free_time, slot, node, orders, up_time, down_time, task, True)

                agent_ready = real_start
                if task in prec:
                    comm_ready = max([self.endtime(p, orders[jobson[p]])
                                      + commcost(p, task, node, jobson[p]) for p in prec[task]])
                else:
                    comm_ready = 0
                return max(agent_ready, comm_ready)
        else:
            """It means task cannot be executed on this resource"""
            return 1000000

    def can_be_executed(self, node, job):
        ## check it
        return (job.soft_reqs in node.soft) or (SoftItem.ANY_SOFT in node.soft)

    def register_vm_for_slot(self, free_time, slot, agent, orders, up_time, down_time, job, only_estimate):
        old_vm = agent.resource.slot_register[slot]
        if not only_estimate:
            agent.resource.slot_register[slot] = agent
        time_to_up = self.down_vm(old_vm, orders, free_time, down_time, only_estimate)
        ready_time = self.up_vm(agent, orders, time_to_up, up_time, job, only_estimate)
        return ready_time


    def down_vm(self, old_vm, orders, free_time, down_time, only_estimate):
        if old_vm == None:
            return free_time
        if len(orders[old_vm]) > 0 and orders[old_vm][-1].job.id == DOWN_JOB.id:
            return free_time
        time_to_up = free_time + down_time
        if not only_estimate:
            down_event = ScheduleItem(DOWN_JOB, free_time, time_to_up)
            orders[old_vm].append(down_event)
            """TODO: it is not right, remake it later"""
            old_vm.state = Node.Down
            old_vm.soft_types = [SoftItem.ANY_SOFT]
        return time_to_up


    def up_vm(self, new_vm, orders, free_time, up_time, job, only_estimate):
        if len(orders[new_vm]) > 0 and orders[new_vm][-1].job.id == UP_JOB.id:
            return free_time
        ready_time = free_time + up_time
        if not only_estimate:
            up_event = ScheduleItem(UP_JOB, free_time, ready_time)
            orders[new_vm].append(up_event)
            """TODO: it is not right, remake it later"""
            new_vm.state = Node.Busy
            new_vm.soft_types = self.build_vm_soft_types(new_vm, job)
        return ready_time

    def build_vm_soft_types(self, vm, job):
        """TODO: extend it later"""
        return [job.soft_reqs]

    def take_free_slot_or_find_freepoint_of_busy(self, node, orders, current_time):
        if node.state != Node.Down:
            raise Exception("agent isn't a ghost, name: " + node.name)

        def filter_lambda(slt):
            return node.resource.slot_register[slt] == None

        free_slots = list(filter(filter_lambda, node.resource))
        if free_slots != list():
            slot = free_slots[0]
            return current_time, slot
        else:
            min_time_of_freeing = 1000000
            minK = -1
            for k, v in node.resource.items():
                end = self.endtime(orders[v][-1].job, orders[v])
                if self.is_not_downing(orders[v][-1]):
                    downing_time = self.downing_time_for(v)
                    end += downing_time
                min_time_of_freeing = end if end < min_time_of_freeing else min_time_of_freeing
                minK = k
        return min_time_of_freeing, minK

    def endtime(self, job, events):
        """ Endtime of job in list of events """
        for e in events:
            if e.job == job:
                return e.end

    def is_not_downing(self,event):
        return event.job.id == DOWN_JOB.id

    def downing_time_for(self,job):
        return 100
