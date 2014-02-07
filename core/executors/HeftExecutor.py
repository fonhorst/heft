from collections import deque
import random
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem
from core.HeftHelper import HeftHelper
from core.executors.EventMachine import EventMachine, TaskStart, NodeFailed, NodeUp, TaskFinished


class HeftExecutor(EventMachine):

    def __init__(self, heft_planner, base_fail_duration, base_fail_dispersion ,
                 initial_schedule = None):
        ## TODO: remake it later
        self.queue = deque()
        self.current_time = 0
        # DynamicHeft
        self.heft_planner = heft_planner
        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        self.initial_schedule = initial_schedule
        self.current_schedule = initial_schedule

    def init(self):
        if self.initial_schedule is None:
            self.current_schedule  = Schedule({node:[] for node in self.heft_planner.get_nodes()})
            self.current_schedule = self.heft_planner.run(self.current_schedule)
        else:
            id_to_task = {tsk.id: tsk for tsk in HeftHelper.get_all_tasks(self.heft_planner.workflow)}
            mapping = {node: [ScheduleItem(id_to_task[item.job.id], item.start_time, item.end_time) for item in items] for (node, items) in self.initial_schedule.mapping.items()}
            self.current_schedule = Schedule(mapping)
        self.post_new_events()

    def event_arrived(self, event):

        def reschedule(event):
            self.heft_planner.current_time = self.current_time
            current_cleaned_schedule = self.clean_events(event)
            self.current_schedule = self.heft_planner.run(current_cleaned_schedule)
            self.post_new_events()

        def check_fail(task, node):
            reliability = self.heft_planner.estimator.estimate_reliability(task, node)
            res = random.random()
            if res > reliability:
                return True
            return False

        if isinstance(event, TaskStart):
            # check task as executing
            # self.current_schedule.change_state(event.task, ScheduleItem.EXECUTING)

            # try to find nodes in cloud


            # check if failed and post
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
            return None
        if isinstance(event, NodeFailed):
            # check node down
            self.heft_planner.resource_manager.node(event.node).state = Node.Down
            # check failed event in schedule
            ## TODO: ambigious choice
            ##self.current_schedule.change_state(event.task, ScheduleItem.FAILED)
            it = [item for item in self.current_schedule.mapping[event.node] if item.job.id == event.task.id and item.state == ScheduleItem.EXECUTING]
            if len(it) != 1:
                ## TODO: raise exception here
                pass

            it[0].state = ScheduleItem.FAILED
            it[0].end_time = self.current_time

            reschedule(event)
            return None
        if isinstance(event, NodeUp):
            # check node up
            self.heft_planner.resource_manager.node(event.node).state = Node.Unknown
            reschedule(event)
            return None
        return None

    def post_new_events(self):
        unstarted_items = set()
        for (node, items) in self.current_schedule.mapping.items():
            for item in items:
                if item.state == ScheduleItem.UNSTARTED:
                    unstarted_items.add(item)

        events_to_post = []
        for item in unstarted_items:
            event_start = TaskStart(item.job)
            event_start.time_happened = item.start_time

            event_finish = TaskFinished(item.job)
            event_finish.time_happened = item.end_time

            events_to_post
            self.post(event_start)
            self.post(event_finish)
        pass

    def clean_events(self, event):

        # remove all unstarted tasks
        cleaned_task = set()
        if isinstance(event, NodeFailed):
            cleaned_task = set([event.task])

        new_mapping = dict()
        for (node, items) in self.current_schedule.mapping.items():
            new_mapping[node] = []
            for item in items:
                if item.state != ScheduleItem.UNSTARTED:
                    new_mapping[node].append(item)
                else:
                    cleaned_task.add(item.job)
        clean_schedule = Schedule(new_mapping)
        # remove all events associated with these tasks
        def check(event):
            if isinstance(event, TaskStart) and event.task in cleaned_task:
                return False
            if isinstance(event, TaskFinished) and event.task in cleaned_task:
                return False
            return True
        self.queue = deque([event for event in self.queue if check(event)])
        return clean_schedule