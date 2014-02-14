from collections import deque


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
    def __init__(self):
        self.queue = deque()
        self.current_time = 0

    def run(self):
        count = 0

        taskStartCount = 0
        nodeFailedCount = 0
        while len(self.queue) > 0:
            event = self.queue.popleft()
            self.current_time = event.time_happened

            # if isinstance(event, TaskStart):
            #     taskStartCount += 1
            # elif isinstance(event, NodeFailed):
            #     nodeFailedCount += 1
            # if isinstance(event, TaskStart):
            #     print(str(count) + " Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(event.task.id) + ' ' + str(None if event.node is None else event.node.name))
            # elif isinstance(event, NodeUp):
            #    print(str(count) + " Event: " + str(event) + ' '+ str(event.time_happened)+ ' ' + str(event.node.name))
            # elif isinstance(event, NodeFailed):
            #     print(str(count) + " Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(event.node.name)+ ' ' + str(event.task.id))
            # elif isinstance(event, TaskFinished):
            #     print(str(count) + " Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(None if event.node is None else event.node.name)+ ' ' + str(event.task.id))
            # else:
              # print(str(count) + " Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(event.task.id))
            count += 1
            self.event_arrived(event)

        # print("===============EventMachine statistics==============")
        # print("  TaskStart events: " + str(taskStartCount))
        # print(" NodeFailed events: " + str(nodeFailedCount))

        pass

    def post(self, event):
        event.time_posted = self.current_time

        # if isinstance(event, TaskStart):
        #         print("Post Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(event.task.id) + ' ' + str(event.node.name))
        # elif isinstance(event, NodeUp):
        #        print("Post Event: " + str(event) + ' '+ str(event.time_happened)+ ' ' + str(event.node.name))
        # elif isinstance(event, NodeFailed):
        #         print( "Post Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(event.node.name)+ ' ' + str(event.task.id))
        # elif isinstance(event, TaskFinished):
        #         print("Post Event: " + str(event) + ' '+ str(event.time_happened) + ' ' + str(event.node.name)+ ' ' + str(event.task.id))


        ## TODO: raise exception if event.time_happened < self.current_time
        if event.time_happened < self.current_time:
            raise Exception("happened time: " + str(event.time_happened) + " is earlier than current time: " + str(self.current_time))
        self.queue.append(event)
        self.queue = deque(sorted(self.queue, key=lambda x: x.time_happened))
        #st = ''
        #for el in self.queue:
        #    st = st + str(el.time_happened) + ' '
        #print(' Queue: ' + st)

    def event_arrived(self, event):
        pass










