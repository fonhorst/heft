__author__ = 'nikolay'

from collections import deque

class BaseEvent:
    def __init__(self):
        self.name = None
        self.id = None


class WfsAddedEvent(BaseEvent):
    def __init__(self):
        self.newWfs = set() ## set new added wfs


class TaskFinished(BaseEvent):
    def __init__(self):
        self.task = None


class TaskFailed(BaseEvent):
    def __init__(self):
        self.task = None
        self.reason = None


class ResourceGoneDown(BaseEvent):
    def __init__(self):
        self.resource = None


class NodeGoneDown(BaseEvent):
    def __init__(self):
        self.node = None


class ResourceGoneUp(BaseEvent):
    def __init__(self):
        self.resource = None


class NodeGoneUp(BaseEvent):
    def __init__(self):
        self.node = None


class TaskLated(BaseEvent):
    def __init__(self):
        self.task = None


class EventAutomata:
    def __init__(self):
        self.queue = deque()
        self.executor = None

    def run(self):
        while len(self.queue) > 0:
            event = self.queue.popleft()
            self.executor.event_arrived(event)

