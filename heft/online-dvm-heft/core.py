__author__ = 'fonhorst'

from functools import partial
from collections import namedtuple
from heft.util import reverse_dict

Event = namedtuple('Event', 'job start end')

Slot = namedtuple('Slot', 'id')



"""
===========================================================================
"""




"""
schedule =
{
id: string
-----------------------------------------------------------
vm0:[Event(time_start, time_end,wf{number}.task{number}),...]
vm1: ...
-----------------------------------------------------------
}

Task={
    id: string
    wf: Workflow
    mips: double
    parents: list of Task
    children: list of Task
}

Workflow={
    id: string
    arrival_time: double
    dag: Task
}

"""

"""
vm0: wf1.task1  wf2.task2

vm1: wf2.task1    wf1.task2

vm2:

"""

class Workflow():
    id = ""
    arrival_time = 0
    dag = {3: (5,),
           4: (6,),
           5: (7,),
           6: (7,),
           7: (8, 9)}
    pass

class Task():
    id = ""
    wf = Workflow()
    mips = 0
    type = ""
    in_to_out_size_func=None
    intra_priority=0

    def __init__(self, id, wf):
        self.id = id
        self.wf = wf
    pass

class Schedule():
    id=""
    plan={
        'vm0':[],
        'vm1':[]
    }

    def __init__(self, id, plan):
        self.id = id
        self.plan = plan
    pass

class Resource():
    id=""
    mips=0
    bw=0
    soft_types=[]
    """
    busy, uping, downing,idle,down
    """
    state = ""
    pass


def rewbar(ni, agents, compcost):
    """ Average computation cost """
    return sum(compcost(ni, agent) for agent in agents) / len(agents)

def recbar(ni, nj, agents, commcost):
    """ Average communication cost """
    n = len(agents)
    if n == 1:
        return 0
    npairs = n * (n-1)
    return 1. * sum(commcost(ni, nj, a1, a2) for a1 in agents for a2 in agents
                                        if a1 != a2) / npairs

def reranku(ni, agents, succ,  compcost, commcost):
    """ Rank of job

    This code is designed to mirror the wikipedia entry.
    Please see that for details

    [1]. http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time
    """
    rank = partial(reranku, compcost=compcost, commcost=commcost,
                           succ=succ, agents=agents)
    w = partial(rewbar, compcost=compcost, agents=agents)
    c = partial(recbar, agents=agents, commcost=commcost)

    if ni in succ and succ[ni]:
        return w(ni) + max(c(ni, nj) + rank(nj) for nj in succ[ni])
    else:
        return w(ni)
def reschedule(wfs, resources, compcost, commcost, current_schedule, time, up_time, down_time):
    """
    without performance variability:
    1. take all unstarted tasks of previous dags
    2. set inter priority for every task of it
    3. create priority(intra and inter) for new incomings
    4. sort by priority decreasing
    5. map all tasks (for ghosts machine calculate time of possible start)
    WITH performance variability:
    """

    """
     create inter-priority
    """
    wf_jobs = {wf: [] for wf in wfs}
    for wf in wfs:
        rank = partial(reranku, agents=resources, succ=wf.dag,
                              compcost=compcost, commcost=commcost)
        jobs = set(wf.dag.keys()) | set(x for xx in wf.dag.values() for x in xx)
        jobs = sorted(jobs, key=rank)
        wf_jobs[wf] = jobs

    old_dag_jobs = get_unstarted_tasks(current_schedule, time)

    all_jobs = dict(old_dag_jobs.items() + wf_jobs.items())

    sorted_jobs = sorted(all_jobs.items(), key=lambda tuple: tuple[0].arrival_time)

    unchanged_schedule = get_unchanged_schedule(current_schedule, time)

    new_schedule = mapping(sorted_jobs, resources, unchanged_schedule, time, up_time, down_time)

    return new_schedule

def get_unstarted_tasks(sched, time):
    wf_jobs = dict()
    for vm, plan in sched.plan.items():
        unstarted_events = filter(function_or_None=lambda evt: evt.start > time, iterable=plan)
        for event in unstarted_events:
            wf_jobs.get(event.job.wf, []).append(event.job)

    for wf in wf_jobs:
        wf_jobs[wf] = sorted(wf_jobs[wf], key=lambda job: job.intra_priority)

    return wf_jobs

def get_unchanged_schedule(sched, time):
    new_plan = dict()
    for vm, plan in sched.plan.items():
        unstarted_events = filter(function_or_None=lambda evt: evt.start < time, iterable=plan)
        new_plan[vm] = unstarted_events

    new_sched = Schedule(id,new_plan)

    return new_sched
"""========================================================="""
def endtime(job, events):
    """ Endtime of job in list of events """
    for e in events:
        if e.job == job:
            return e.end

def re_start_time(job, orders, jobson, prec, commcost, agent):
    """
        (We work without any window)
        1. check for correspondence of resource and job
        2. if resource is running(the last block in the old schedule is just a run), find first possible free time and put entry there
            - else if resource is up, find first possible free time and put entry there (it should be performed to find a better solution)
            - else if resource is down, add up-block and put entry next (it should be performed to find a better solution)
        3. if resource is ghost and there is a free slot, take the free slot and up the ghost,
        4. if resource is ghost, find first time of freeing a slot, take it and schedule uping of the ghost
    """

    if (agent.soft_types.contains(job)):
        if (agent.state != "ghost"):
            agent_ready = orders[agent][-1].end if orders[agent] else 0
            if job in prec:
                comm_ready = max([endtime(p, orders[jobson[p]])
                       + commcost(p, job, agent, jobson[p]) for p in prec[job]])
            else:
                comm_ready = 0
            return max(agent_ready, comm_ready)
        else:
            free_time, slot = take_free_slot_or_find_freepoint_of_busy(agent)
            """
                register_vm_for_slot(free_time, slot, agent)
            """

            agent_ready = free_time
            if job in prec:
                comm_ready = max([endtime(p, orders[jobson[p]])
                       + commcost(p, job, agent, jobson[p]) for p in prec[job]])
            else:
                comm_ready = 0
            return max(agent_ready, comm_ready)
    else:
        return 1000000

slot_register={
    Slot("slot_1"):'vm0',
    Slot("slot_2"):'vm1',
    Slot("slot_3"):'vm2'
}

UP_JOB = Task("up_job", None)

DOWN_JOB = Task("down_job", None)

def register_vm_for_slot(free_time, slot, agent, orders):
    old_vm = slot_register[slot]
    slot_register[slot] = agent
    down_vm(old_vm, orders)
    up_vm(agent, orders)

def down_vm(old_vm, orders):
    if orders[old_vm][-1].id == DOWN_JOB.id:
        return
    orders[old_vm].append(DOWN_JOB)

def up_vm(old_vm, orders):
    if orders[old_vm][-1].id == UP_JOB.id:
        return
    orders[old_vm].append(UP_JOB)


def take_free_slot_or_find_freepoint_of_busy(agent, orders, current_time):
    if (agent.state != "ghost"):
        raise Exception("agent isn't a ghost, id: " + agent.id)
    free_slots = filter(lambda tuple: tuple.value == "", slot_register)
    if (free_slots != list()):
        """slot_register[free_slots[0].key] = agent"""
        slot = free_slots[0].key
        return current_time, slot
    else:
        min_time_of_freeing = 1000000
        minK = -1
        for k, v in slot_register.items():
            end = endtime(orders[v][-1],orders[v])
            if is_not_downing(orders[v][-1]):
                downing_time = downing_time_for(v)
                end = end + downing_time
            min_time_of_freeing = end if end < min_time_of_freeing else min_time_of_freeing
            minK = k
        """
        old_vm = slot_register[minK]
        slot_register[minK] = agent
        down_vm(old_vm)
        """
        return min_time_of_freeing, minK

def is_not_downing(job):
    return job.id == DOWN_JOB.id

def downing_time_for(job):
    return 100

"""========================================================="""

def get_next_sched_id():
    return -1;

def mapping(sorted_jobs, resources, unchanged_schedule, commcost, compcost, time, up_time, down_time):
    """def allocate(job, orders, jobson, prec, compcost, commcost):"""
    """ Allocate job to the machine with earliest finish time

    Operates in place
    """

    jobson = dict()

    new_plan = dict(unchanged_schedule.plan)

    for wf, jobs in sorted_jobs:
        prec = reverse_dict(wf.dag)
        for job in jobs:
            st = partial(re_start_time, job, new_plan, jobson, prec, commcost)
            ft = lambda machine: st(machine) + compcost(job, machine)
            agent = min(new_plan.keys(), key=ft)
            start = st(agent)
            end = ft(agent)

            if agent.state == "ghost":
                free_time, slot = take_free_slot_or_find_freepoint_of_busy(agent)
                register_vm_for_slot(free_time, slot, agent,new_plan)

            new_plan[agent].append(Event(job, start, end))
            jobson[job] = agent

    new_id = get_next_sched_id()
    new_sched = Schedule(new_id, new_plan)
    return new_sched

"""
===============================================================
"""
