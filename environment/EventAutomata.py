__author__ = 'nikolay'

from collections import deque

class BaseEvent:
    def __init__(self, id, time_posted, time_happened):
        self.id = id
        self.time_posted = time_posted
        self.time_happened = time_happened

class PostingEntity:
    def post(self, event):
        pass

class EventAutomata(PostingEntity):
    def __init__(self, executor):
        self.queue = deque()
        self.executor = executor
        self.executor.posting_entity = self

    def run(self):
        while len(self.queue) > 0:
            event = self.queue.popleft()
            self.executor.event_arrived(event)

    def post(self, event):
        self.queue.append(event)
        sorted(self.queue, key=lambda x: x.time_happened)

