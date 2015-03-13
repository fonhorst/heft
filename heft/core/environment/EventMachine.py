from collections import deque
from heft.utilities.common import trace


class BaseEvent:
    def __init__(self, id, time_posted, time_happened):
        self.id = id
        self.time_posted = time_posted
        self.time_happened = time_happened

class TaskStart(BaseEvent):
    def __init__(self, task):
        self.task = task
        self.node = None

    def __str__(self):
        return "TaskStart"

class TaskFinished(BaseEvent):
    def __init__(self, task):
        self.task = task
        self.node = None
    def __str__(self):
        return "TaskFinished"

class NodeFailed(BaseEvent):
    def __init__(self, node, task):
        self.node = node
        self.task = task

    def __str__(self):
        return "NodeFailed"

class NodeUp(BaseEvent):
    def __init__(self, node):
        self.node = node

    def __str__(self):
        return "NodeUp"

class EventMachine:
    #@trace
    def __init__(self):
        self.queue = deque()
        self.current_time = 0
        self._stopped = False

    def run(self):
        count = 0
        while len(self.queue) > 0:

            if hasattr(self, "_stopped") and self._stopped is True:
                print("EventMachine has been stopped")
                break

            event = self.queue.popleft()

            if self.current_time > event.time_happened:
                raise Exception('current_time > event.time_happened: ' + str(self.current_time) + ' > ' + str(event.time_happened))

            self.current_time = event.time_happened
            count += 1
            self.event_arrived(event)

        pass

    def post(self, event):
        event.time_posted = self.current_time
        if event.time_happened < self.current_time:
            raise Exception("happened time: " + str(event.time_happened) + " is earlier than current time: " + str(self.current_time))
        self.queue.append(event)
        self.queue = deque(sorted(self.queue, key=lambda x: x.time_happened))

    def stop(self):
        self._stopped = True

    def event_arrived(self, event):
        pass










