__author__ = 'fonhorst'

from functools import partial
from collections import namedtuple
from heft.util import reverse_dict

Event = namedtuple('Event', 'job start end')

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
    in_to_out_size_func=None
    intra_priority=0
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

def endtime(job, events):
    """ Endtime of job in list of events """
    for e in events:
        if e.job == job:
            return e.end

def re_start_time(job, orders, jobson, prec, commcost, agent):
    """ Earliest time that job can be executed on agent """
    agent_ready = orders[agent][-1].end if orders[agent] else 0
    if job in prec:
        comm_ready = max([endtime(p, orders[jobson[p]])
                       + commcost(p, job, agent, jobson[p]) for p in prec[job]])
    else:
        comm_ready = 0
    return max(agent_ready, comm_ready)

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
            new_plan[agent].append(Event(job, start, end))
            jobson[job] = agent

    new_id = get_next_sched_id()
    new_sched = Schedule(new_id, new_plan)
    return new_sched

"""
===============================================================
"""
