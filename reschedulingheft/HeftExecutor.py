from collections import deque
import random
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem


class BaseEvent:
    def __init__(self, id, time_posted, time_happened):
        self.id = id
        self.time_posted = time_posted
        self.time_happened = time_happened

class TaskStart(BaseEvent):
    def __init__(self, task):
        self.task = task

class TaskFinished(BaseEvent):
    def __init__(self, task):
        self.task = task

class NodeFailed(BaseEvent):
    def __init__(self, node, task):
        self.node = node
        self.task = task

class NodeUp(BaseEvent):
    def __init__(self, node):
        self.node = node

class EventMachine:
    def __init__(self, executor):
        self.queue = deque()
        self.current_time = 0

    def run(self):
        #count = 0
        while len(self.queue) > 0:
            event = self.queue.popleft()
            #print(str(count) + " Event: " + str(event.time_happened) + ' ' + str(event.task.id))
            #count += 1
            self.event_arrived(event)

    def post(self, event):
        event.time_posted = self.current_time
        self.queue.append(event)
        self.queue = deque(sorted(self.queue, key=lambda x: x.time_happened))
        #st = ''
        #for el in self.queue:
        #    st = st + str(el.time_happened) + ' '
        #print(' Queue: ' + st)

    def event_arrived(self, event):
        pass


class HeftExecutor(EventMachine):

    def __init__(self, heft_planner, ):
        # DynamicHeft
        self.heft_planner = heft_planner
        self.current_schedule = Schedule({node:[] for node in heft_planner.get_nodes()})

    def event_arrived(self, event):

        def reschedule(event):
            current_cleaned_schedule = self.clean_events(event)
            self.current_schedule = self.heft_planner.run(current_cleaned_schedule)
            self.post_new_events()

        def check_fail(task, node):
            reliability = self.estimator.estimate_reliability(task, node)
            res = random.random()
            if res > reliability:
                return True
            return False

        if isinstance(event, TaskStart):
            # check task as executing
            self.current_schedule.change_state(event.task, ScheduleItem.EXECUTING)
            # check if failed and post
            (node, item) = self.current_schedule.place(event.task)

            if check_fail(event.task, node):
                # generate fail time, post it
                duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
                time_of_fail = (item.end_time - self.current_time)*random.random()
                time_of_fail = self.current_time + time_of_fail if time_of_fail > 0 else (item.end_time - self.current_time)*0.01

                event_failed = NodeFailed(node, event.task)
                event.time_happened = time_of_fail
                self.post(event)
                # remove TaskFinished event
                self.queue = deque([ev for ev in self.queue if not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id)])

                pass
            return None
        if isinstance(event, TaskFinished):
            # check task finished
            self.current_schedule.change_state(event.task, ScheduleItem.FINISHED)
            return None
        if isinstance(event, NodeFailed):
            # check node down
            self.heft_planner.resource_manager.node(event.node).state = Node.Down
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
        for item in unstarted_items:
            event_start = TaskStart(item.job)
            event_start.time_happened = item.start_time

            event_finish = TaskFinished(item.job)
            event_finish.time_happened = item.end_time

            self.post(event_start)
            self.post(event_finish)
        pass

    def clean_events(self, event):
        # check failed event in schedule
        self.current_schedule(event.task, ScheduleItem.FAILED)
        # remove all unstarted tasks
        cleaned_task = set()
        if isinstance(event, NodeFailed):
            cleaned_task = set([event.task])

        new_mapping = dict()
        for (node, items) in self.current_schedule.mapping.items():
            for item in items:
                if item.state != ScheduleItem.UNSTARTED:
                    lst = new_mapping.get(node, [])
                    lst.append(item)
                    new_mapping[node] = lst
                else:
                    cleaned_task.add(item.job)
        clean_schedule = Schedule(new_mapping)
        # remove all events associated with these tasks
        def check(event):
            if isinstance(event, TaskStart) and event.task in cleaned_task:
                return True
            if isinstance(event, TaskFinished) and event.task in cleaned_task:
                return True
            return False
        self.queue = deque([check(event) for event in self.queue])
        return clean_schedule










