__author__ = 'fonhorst'

from functools import partial
from collections import namedtuple
from heft.util import reverse_dict

Event = namedtuple('Event', 'job start end')

Slot = namedtuple('Slot', 'id')

State = namedtuple('State', 'name')

Soft = namedtuple('Soft', 'name')

Busy = State('Busy')
Uping = State('Uping')
Downing = State('Downing')
Idle = State('Idle')
Down = State('Down')

ANY_SOFT = Soft('Any_soft')
Windows7 = Soft('Windows7')
Linux = Soft('Linux')
WindowsR2 = Soft('WindowsR2')

"""
for testing only
=========================
"""
OS_1 = Soft('OS_1')
OS_2 = Soft('OS_2')
OS_3 = Soft('OS_3')
OS_4 = Soft('OS_4')
OS_5 = Soft('OS_5')
OS_6 = Soft('OS_6')
OS_7 = Soft('OS_7')
"""
=========================
"""

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


class Factory():
    resId = 0

    tasks =dict()

    def t(self,id):
        return self.tasks[id]

    def createTask(self, id, wf, mips):
        if id not in self.tasks:
            self.tasks[id] = Task(id, wf, mips)
        return self.tasks[id]

    def createWf(self):
        wf = Workflow("wf1", None)
        t = partial(self.createTask, wf=wf, mips=2000)
        dag = {t("3"): (t("5"),),
               t("4"): (t("6"),),
               t("5"): (t("7"),),
               t("6"): (t("7"),),
               t("7"): (t("8"), t("9"))}
        wf.dag = dag
        return wf

    def createDiffWf(self):
        wf = Workflow("wf1", None)
        t = partial(self.createTask, wf=wf, mips=2000)
        dag = {t("3"): (t("5"),),
               t("4"): (t("6"),),
               t("5"): (t("7"),),
               t("6"): (t("7"),),
               t("7"): (t("8"), t("9"))}

        self.t("3").type = OS_1
        self.t("4").type = OS_2
        self.t("5").type = OS_3
        self.t("6").type = OS_4
        self.t("7").type = OS_5
        self.t("8").type = OS_6
        self.t("9").type = OS_7

        wf.dag = dag
        return wf

    def createResource(self):
        id = "vm" + self.resId
        self.resId += 1
        return Resource(id, Down)


class Workflow():
    id = ""
    arrival_time = 0
    dag = None

    def __init__(self, id, dag):
        self.id = id
        self.dag = dag


class Task():
    id = ""
    wf = None
    mips = 0
    type = ""
    in_to_out_size_func = None
    intra_priority = 0

    def __init__(self, id, wf, mips):
        self.id = id
        self.wf = wf
        self.mips = mips

    def __str__(self):
        return self.str()

    def __repr__(self):
        return self.str()

    def str(self):
        if self.id == UP_JOB.id or self.id == DOWN_JOB.id:
            return self.id
        return self.wf.id + "." + self.id

class Schedule():
    id = ""
    plan = {
        'vm0': [],
        'vm1': []
    }

    def __init__(self, id, plan):
        self.id = id
        self.plan = plan


class Resource():
    id = ""
    mips = 0
    bw = 0
    soft_types = []
    state = None

    def __init__(self, id, state, mips):
        self.id = id
        self.state = state
        self.mips = mips

    def __str__(self):
        return self.id

    def __repr__(self):
        return self.id


def compcost(job, agent):
    return (job.mips / agent.mips) * 10


def commcost(ni, nj, A, B):
    if A == B:
        return 0
    """TODO: remake it later"""
    return 100

"""
Right schedule for commcost 100(I think in that way):
vm0		Event(job=up_job, start=0, end=20)
	Event(job=wf1.4, start=20, end=40.0)
	Event(job=wf1.6, start=40.0, end=60.0)
	Event(job=wf1.7, start=160.0, end=180.0)
	Event(job=wf1.9, start=180.0, end=200.0)
	Event(job=wf1.8, start=200.0, end=220.0)
vm1		Event(job=up_job, start=0, end=20)
	Event(job=wf1.3, start=20, end=40.0)
	Event(job=wf1.5, start=40.0, end=60.0)
vm2

Right schedule for commcost 10(I think in that way):
vm0		Event(job=up_job, start=0, end=20)
	Event(job=wf1.4, start=20, end=40.0)
	Event(job=wf1.6, start=40.0, end=60.0)
	Event(job=wf1.7, start=70.0, end=90.0)
	Event(job=wf1.9, start=90.0, end=110.0)
vm1		Event(job=up_job, start=0, end=20)
	Event(job=wf1.3, start=20, end=40.0)
	Event(job=wf1.5, start=40.0, end=60.0)
	Event(job=wf1.8, start=100.0, end=120.0)
vm2
"""

def rewbar(ni, agents, compcost):
    """ Average computation cost """
    return sum(compcost(ni, agent) for agent in agents) / len(agents)


def recbar(ni, nj, agents, commcost):
    """ Average communication cost """
    n = len(agents)
    if n == 1:
        return 0
    npairs = n * (n - 1)
    return 1. * sum(commcost(ni, nj, a1, a2) for a1 in agents for a2 in agents
                    if a1 != a2) / npairs


def reranku(ni, agents, succ, compcost, commcost):
    """ Rank of job

    This code is designed to mirror the wikipedia entry.
    Please see that for details

    [1]. http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time
    """
    rank = partial(reranku, compcost=compcost, commcost=commcost,
                   succ=succ, agents=agents)
    w = partial(rewbar, compcost=compcost, agents=agents)
    c = partial(recbar, agents=agents, commcost=commcost)

    result = None

    if ni in succ and succ[ni]:
        result = w(ni) + max(c(ni, nj) + rank(nj) for nj in succ[ni])
    else:
        result = w(ni)

    """print( "%s %s" % (ni, result))"""

    return result


def reschedule(wfs, resources, compcost, commcost, current_schedule, time, up_time, down_time, generate_new_ghost_machine):
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
        wf_jobs[wf] =list(reversed(jobs))

    old_dag_jobs = get_unstarted_tasks(current_schedule, time)

    all_jobs = old_dag_jobs.copy()
    all_jobs.update(wf_jobs)
    """set(wf.dag.set(x for xx in wf.dag.values() for x in xx)keys())"""
    sorted_jobs = sorted(all_jobs.items(), key=lambda tuple: tuple[0].arrival_time)

    unchanged_schedule = get_unchanged_schedule(current_schedule, time)

    new_schedule = mapping(sorted_jobs, resources, unchanged_schedule, commcost, compcost, time, up_time, down_time, generate_new_ghost_machine)

    return new_schedule


def get_unstarted_tasks(sched, time):
    wf_jobs = dict()
    for vm, plan in sched.plan.items():
        unstarted_events = list(filter(lambda evt: evt.start > time, plan))
        for event in unstarted_events:
            wf_jobs.get(event.job.wf, []).append(event.job)

    for wf in wf_jobs:
        wf_jobs[wf] = sorted(wf_jobs[wf], key=lambda job: job.intra_priority)

    return wf_jobs


def get_unchanged_schedule(sched, time):
    new_plan = dict()
    for vm, plan in sched.plan.items():
        unstarted_events = list(filter(lambda evt: evt.start < time, plan))
        new_plan[vm] = unstarted_events

    new_sched = Schedule(id, new_plan)

    return new_sched


"""========================================================="""


def endtime(job, events):
    """ Endtime of job in list of events """
    for e in events:
        if e.job == job:
            return e.end


def re_start_time(job, orders, jobson, prec, commcost, up_time, down_time, time, agent):
    """
        (We work without any window)
        1. check for correspondence of resource and job
        2. if resource is running(the last block in the old schedule is just a run), find first possible free time and put entry there
            - else if resource is up, find first possible free time and put entry there (it should be performed to find a better solution)
            - else if resource is down, add up-block and put entry next (it should be performed to find a better solution)
        3. if resource is ghost and there is a free slot, take the free slot and up the ghost,
        4. if resource is ghost, find first time of freeing a slot, take it and schedule uping of the ghost
    """

    if can_be_executed(agent, job):
        if agent.state is not Down:
            agent_ready = orders[agent][-1].end if orders[agent] else 0
            if job in prec:
                comm_ready = max([endtime(p, orders[jobson[p]])
                                  + commcost(p, job, agent, jobson[p]) for p in prec[job]])
            else:
                comm_ready = 0
            return max(agent_ready, comm_ready)
        else:
            free_time, slot = take_free_slot_or_find_freepoint_of_busy(agent, orders, time)
            real_start = register_vm_for_slot(free_time, slot, agent, orders, up_time, down_time, job, True)
            """
                register_vm_for_slot(free_time, slot, agent)
            """

            agent_ready = real_start
            if job in prec:
                comm_ready = max([endtime(p, orders[jobson[p]])
                                  + commcost(p, job, agent, jobson[p]) for p in prec[job]])
            else:
                comm_ready = 0
            return max(agent_ready, comm_ready)
    else:
        """TODO: remake it later. It means task cannot be executed on this resource"""
        return 1000000


"""
slot_register={
    Slot("slot_1"):'vm0',
    Slot("slot_2"):'vm1',
    Slot("slot_3"):'vm2'
}
"""

slot_register = {
    Slot("slot_1"): None,
    Slot("slot_2"): None,
    Slot("slot_3"): None
}

UP_JOB = Task("up_job", None, -1)

DOWN_JOB = Task("down_job", None, -1)


def register_vm_for_slot(free_time, slot, agent, orders, up_time, down_time, job, only_estimate):
    old_vm = slot_register[slot]
    if not only_estimate:
        slot_register[slot] = agent
    time_to_up = down_vm(old_vm, orders, free_time, down_time, only_estimate)
    ready_time = up_vm(agent, orders, time_to_up, up_time, job, only_estimate)
    return ready_time


def down_vm(old_vm, orders, free_time, down_time, only_estimate):
    if old_vm == None:
        return free_time
    if len(orders[old_vm]) > 0 and orders[old_vm][-1].job.id == DOWN_JOB.id:
        return free_time
    time_to_up = free_time + down_time
    if not only_estimate:
        down_event = Event(DOWN_JOB, free_time, time_to_up)
        orders[old_vm].append(down_event)
        """TODO: it is not right, remake it later"""
        old_vm.state = Down
        old_vm.soft_types = [ANY_SOFT]
    return time_to_up


def up_vm(new_vm, orders, free_time, up_time, job, only_estimate):
    if len(orders[new_vm]) > 0 and orders[new_vm][-1].job.id == UP_JOB.id:
        return free_time
    ready_time = free_time + up_time
    if not only_estimate:
        up_event = Event(UP_JOB, free_time, ready_time)
        orders[new_vm].append(up_event)
        """TODO: it is not right, remake it later"""
        new_vm.state = Busy
        new_vm.soft_types = build_vm_soft_types(new_vm, job)
    return ready_time

def build_vm_soft_types(vm, job):
    """TODO: extend it later"""
    return [job.type]

def take_free_slot_or_find_freepoint_of_busy(agent, orders, current_time):
    if agent.state != Down:
        raise Exception("agent isn't a ghost, id: " + agent.id)

    def filter_lambda(slt):
        return slot_register[slt] == None

    free_slots = list(filter(filter_lambda, slot_register))
    if free_slots != list():
        """slot_register[free_slots[0].key] = agent"""
        slot = free_slots[0]
        return current_time, slot
    else:
        min_time_of_freeing = 1000000
        minK = -1
        for k, v in slot_register.items():
            end = endtime(orders[v][-1].job, orders[v])
            if is_not_downing(orders[v][-1]):
                downing_time = downing_time_for(v)
                end += downing_time
            min_time_of_freeing = end if end < min_time_of_freeing else min_time_of_freeing
            minK = k
        """
        old_vm = slot_register[minK]
        slot_register[minK] = agent
        down_vm(old_vm)
        """
        return min_time_of_freeing, minK


def is_not_downing(event):
    return event.job.id == DOWN_JOB.id


def downing_time_for(job):
    return 100


"""========================================================="""


def get_next_sched_id():
    return -1;


def mapping(sorted_jobs, resources, unchanged_schedule, commcost, compcost, time, up_time, down_time, generate_new_ghost_machine):
    """def allocate(job, orders, jobson, prec, compcost, commcost):"""
    """ Allocate job to the machine with earliest finish time

    Operates in place
    """

    jobson = dict()

    new_plan = dict(unchanged_schedule.plan)

    def ft(machine):
        cost = st(machine) + compcost(job, machine)
        print("machine: %s job:%s cost: %s" % (machine.id, job.id, cost))
        return cost

    for wf, jobs in sorted_jobs:
        prec = reverse_dict(wf.dag)
        for job in jobs:
            print("======================")
            st = partial(re_start_time, job, new_plan, jobson, prec, commcost, up_time, down_time, time)
            """ft = lambda machine: st(machine) + compcost(job, machine)"""


            agent = min(new_plan.keys(), key=ft)
            """
            if agent.cost >= 100000:
                ghost = geberate_new_ghost_machine
                new_plan[ghost] = []
                print("Creating new ghost: " + ft(ghost))
                agent = ghost
            """

            start = st(agent)
            end = ft(agent)

            if start >= 1000000:
                ghost = generate_new_ghost_machine()
                new_plan[ghost] = []
                ghost.soft_types = [ANY_SOFT]
                print("Creating new ghost: %s cost: %s" % (ghost.id, ft(ghost)))
                agent = ghost
                start = st(agent)
                end = ft(agent)


            if agent.state == Down:
                free_time, slot = take_free_slot_or_find_freepoint_of_busy(agent, new_plan, time)
                """TODO: remake it later"""
                start = max(start, register_vm_for_slot(free_time, slot, agent, new_plan, up_time, down_time, job, False))
                end = start + compcost(job, agent)

            new_plan[agent].append(Event(job, start, end))
            jobson[job] = agent

    new_id = get_next_sched_id()
    new_sched = Schedule(new_id, new_plan)
    return new_sched


def can_be_executed(resource, job):
    return (job.type in resource.soft_types) or (ANY_SOFT in resource.soft_types)


"""
===============================================================
"""
