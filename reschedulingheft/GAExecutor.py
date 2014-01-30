from collections import deque
import random
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem
from reschedulingheft.HeftExecutor import EventMachine, TaskStart, TaskFinished, NodeFailed, NodeUp


class GAExecutor(EventMachine):

    def __init__(self, workflow, resource_manager, estimator, initial_schedule, base_fail_duration, base_fail_dispersion):
        ## TODO: remake it later
        self.queue = deque()
        self.current_time = 0
        self.workflow = workflow
        # DynamicHeft
        #self.heft_planner = heft_planner
        self.resource_manager = resource_manager
        self.estimator = estimator
        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        ##self.current_schedule = Schedule({node:[] for node in heft_planner.get_nodes()})
        self.initial_schedule = initial_schedule
        self.current_schedule = Schedule({key:[] for key in initial_schedule.mapping.keys()})

        #self.ready_tasks = []
        self.finished_tasks = [self.workflow.head_task.id]


    def init(self):
        #self.current_schedule = self.heft_planner.run(self.current_schedule)

        #to_run = [child for child in self.workflow.head_task.children if self.is_next_to_run(child)]
        unstarted_tasks = self.get_ready_tasks(self.workflow.head_task, None)
        #run ready tasks
        self.post_new_events(unstarted_tasks)

    def is_ready(self, task):
        nope = False in [(p.id in self.finished_tasks) for p in task.parents]
        return not nope

    def is_next_to_run(self, task):
        (node, item) = self.initial_schedule.place(task)
        its = [it for it in self.initial_schedule.mapping[node] if it.start_time < item.start_time]
        not_next = False in [(it.job.id in self.finished_tasks) for it in its]
        return not not_next

    def event_arrived(self, event):
        def check_fail(task, node):
            ## TODO: remove it later
            #return False
            reliability = self.estimator.estimate_reliability(task, node)
            res = random.random()
            if res > reliability:
                return True
            return False

        if isinstance(event, TaskStart):
            (node, item) = self.current_schedule.place_by_time(event.task, event.time_happened)
            item.state = ScheduleItem.EXECUTING

            if check_fail(event.task, node):
                # generate fail time, post it
                duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
                time_of_fail = (item.end_time - self.current_time)*random.random()
                time_of_fail = self.current_time + (time_of_fail if time_of_fail > 0 else 0.01) ##(item.end_time - self.current_time)*0.01

                event_failed = NodeFailed(node, event.task)
                event_failed.time_happened = time_of_fail

                event_nodeup = NodeUp(node)
                event_nodeup.time_happened = time_of_fail + duration

                self.post(event_failed)
                self.post(event_nodeup)
                # remove TaskFinished event
                self.queue = deque([ev for ev in self.queue if not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id)])

                pass
            return None
        if isinstance(event, TaskFinished):
            # check task finished

            self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)

            self.finished_tasks.append(event.task.id)

            unstarted_items = self.get_ready_tasks(event.task, event.node)

            ##TODO: remove it later
            #print("==============================")
            #print("Task " + str(event.task) + " finished")
            #for item in unstarted_items:
            #    print("Start task: " + str(item.job) + " On node: " + str(self.initial_schedule.place(item.job)[0]))
            #print("==============================")
            #generate new task start events
            self.post_new_events(unstarted_items)
            return None
        if isinstance(event, NodeFailed):
            # check node down
            self.resource_manager.node(event.node).state = Node.Down
            # check failed event in schedule
            ## TODO: ambigious choice
            ##self.current_schedule.change_state(event.task, ScheduleItem.FAILED)
            it = [item for item in self.current_schedule.mapping[event.node] if item.job.id == event.task.id and item.state == ScheduleItem.EXECUTING]
            if len(it) != 1:
                ## TODO: raise exception here
                pass

            it[0].state = ScheduleItem.FAILED
            it[0].end_time = self.current_time

            return None
        if isinstance(event, NodeUp):
            # check node up
            self.resource_manager.node(event.node).state = Node.Unknown
            #get next task for this node
            next_sched_item = []
            for item in self.initial_schedule.mapping[event.node]:
                if item.job.id not in self.finished_tasks:
                    next_sched_item = item
                    break

            runtime = next_sched_item.end_time - next_sched_item.start_time
            start_time = self.current_time
            end_time = start_time + runtime

            actual_sched_item = ScheduleItem(next_sched_item.job, start_time, end_time)
            self.post_new_events([actual_sched_item])

            return None
        return None

    def get_ready_tasks(self, ptask, pnode):
        unstarted_items = []
        next_for_ptask = self.initial_schedule.get_next_item(ptask)
        #next_for_ptask = [] if next_for_ptask is None else [next_for_ptask.job]
        tsks = [tsk for tsk in ptask.children if self.is_ready(tsk) and self.is_next_to_run(tsk)]
        ##TODO: refactor it later
        if next_for_ptask is not None and next_for_ptask.job not in tsks and self.is_ready(next_for_ptask.job) and self.is_next_to_run(next_for_ptask.job):
            tsks.append(next_for_ptask.job)

        # tsks mustn't be finished, executing or their node is Down
        def appropriate_to_run(tsk):
            if tsk.id in self.finished_tasks:
                return False
            if self.current_schedule.is_executing(tsk):
                return False
            nd = self.initial_schedule.place(tsk)[0]
            if self.resource_manager.node(nd).state == Node.Down:
                return False
            return True

        tsks = [tsk for tsk in tsks if appropriate_to_run(tsk)]

        for child in tsks:
            (node, item) = self.initial_schedule.place(child)

            ## TODO: remake it later
            transf = 0 if pnode is None else self.estimator.estimate_transfer_time(pnode, node, ptask, child)

            runtime = item.end_time - item.start_time
            start_time = self.current_time + transf
            end_time = start_time + runtime

            actual_sched_item = ScheduleItem(item.job, start_time, end_time)
            unstarted_items.append(actual_sched_item)
        return unstarted_items

    def post_new_events(self, unstarted_items):
        for item in unstarted_items:
            (node, it) = self.initial_schedule.place(item.job)

            event_start = TaskStart(item.job)
            event_start.time_happened = item.start_time
            event_start.node = node

            event_finish = TaskFinished(item.job)
            event_finish.time_happened = item.end_time
            event_finish.node = node

            self.post(event_start)
            self.post(event_finish)

            self.current_schedule.mapping[node].append(item)
        pass












