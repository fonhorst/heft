from collections import deque
from pprint import pprint
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
        return "TaskStart: {id}-{node}-{time:.2f}".format(id=self.task.id, node=self.node.name, time=self.time_happened)

    def __repr__(self):
        return self.__str__()


class TaskFinished(BaseEvent):
    def __init__(self, task):
        self.task = task
        self.node = None

    def __str__(self):
        return "TaskFinished: {id}-{node}-{time:.2f}".format(id=self.task.id, node=self.node.name, time=self.time_happened)

    def __repr__(self):
        return self.__str__()


class NodeFailed(BaseEvent):
    def __init__(self, node, task):
        self.node = node
        self.task = task

    def __str__(self):
        return "NodeFailed"

class NodeUp(BaseEvent):
    def __init__(self, node, failed_event):
        self.node = node
        self.failed_event = failed_event

    def __str__(self):
        return "NodeUp"

class ResourceFailed(BaseEvent):
    def __init__(self, resource, task):
        self.resource = resource
        self.task = task

    def __str__(self):
        return "ResourceFailed"

class ResourceUp(BaseEvent):
    def __init__(self, resource, failed_event):
        self.resource = resource
        self.failed_event = failed_event

    def __str__(self):
        return "ResourceUp"


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

        # TODO: debug
        if event.time_happened >= 1000000:
            pprint(self.current_schedule.mapping)
            raise Exception("I'm here")

        self.queue.append(event)
        self.queue = deque(sorted(self.queue, key=lambda x: x.time_happened))

    def stop(self):
        self._stopped = True

    def event_arrived(self, event):
        pass










