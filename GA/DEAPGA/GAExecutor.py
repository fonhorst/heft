import random
from environment.EventAutomata import BaseEvent
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem


class TaskFinished(BaseEvent):
    def __init__(self):
        self.task = None
        self.node = None


class NodeFailed(BaseEvent):
    def __init__(self):
        self.task = None
        self.node = None
        self.duration = None

class NodeUp(BaseEvent):
    def __init__(self):
        self.node = None
        self.failed_task = None

##interface Executor
class Executor:
    def __init__(self):
        self.posting_entity = None

    def event_arrived(self, event):
        pass

    def post_event(self, event):
        self.posting_entity.post(event)
    pass

class GAExecutor(Executor):
    def __init__(self, initial_schedule, estimator, resource_manager,  base_fail_duration, base_fail_dispersion):
        self.initial_schedule = initial_schedule
        self.schedule = Schedule({node: [] for node in initial_schedule.mapping.keys()})
        self.estimator = estimator
        self.resource_manager = resource_manager
        self.base_fail_duration = base_fail_duration
        ## TODO: now only positive dispersion works. Correct it later.
        self.base_fail_dispersion = base_fail_dispersion
        pass

    def event_arrived(self, event):
        if isinstance(event, TaskFinished):

            ##check_valid_event()

            current_time = event.time_happened
            to_run = []
            task = event.task
            node = event.node
            # get next task to run on this node
            nt = self.initial_schedule.get_next_item(task)
            if self.check_run(nt, node, current_time):
                to_run.append(nt)

            for child in task.children:
                if self.check_run(child, self.initial_schedule.place(child), current_time):
                    to_run.append(child)

            for tk in to_run:
                (node, scitem) = self.initial_schedule.place(tk)
                self.schedule.mapping[node].append(scitem)
                self.run(node,scitem,current_time)

            ## 1. obtain ready tasks
            ## 2. for every task check if it can be started
            ## 3. modify schedule
            ## 4. run it
            return None
        if isinstance(event, NodeFailed):
            ## 1. return failed task to unstarted state
            task = event.task
            node = event.node
            current_time = event.time_happened
            self.schedule.change_state(task, ScheduleItem.FAILED)
            self.resource_manager.node(node).state = Node.Down

            self.generate_nodeup(event, current_time)
            ##TODO: generate proper event when use run func
            ##invalidate_linked_events()
            return None
        if isinstance(event, NodeUp):
            ## 1. get ready tasks for this node
            ## 2. run it
            current_time = event.time_happened
            to_run = []
            task = event.failed_task
            node = event.node
            nt = self.initial_schedule.get_next_item(task)
            if self.check_run(nt, node, current_time):
                to_run.append(nt)

            for tk in to_run:
                (node, scitem) = self.initial_schedule.place(tk)
                self.schedule.mapping[node].append(scitem)
                self.run(node,scitem,current_time)
            return None

    def check_run(self, nt, node, current_time):
        def check(p):
            result = self.schedule.place(p)
            if result is None:
                return False
            (pnode, item) = result
            if item.state != ScheduleItem.FINISHED:
                return False
            ttime = self.estimator.estimate_transfer_time(pnode, node, nt.job, item.job)
            return item.end_time + ttime <= current_time

        result = False in [check(p) for p in nt.job.parents]
        if result is True:
            return False
        return True

    def run(self,node, scitem, current_time):

        def normal_run(end_time):
            item = ScheduleItem(scitem.job, current_time, end_time)
            item.state = ScheduleItem.EXECUTING
            self.schedule.mapping[node].append(item)
            ## TODO: generate id here
            event = TaskFinished()
            event.time_posted = current_time
            event.time_happened = end_time
            event.job = scitem.job
            event.node = node
            self.post_event(event)

        def failed_run(end_time):
            duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
            time_of_fail = (end_time - current_time)*random.random()
            end_time = current_time + time_of_fail if time_of_fail > 0 else (end_time - current_time)*0.01
            item = ScheduleItem(scitem.job, current_time, end_time)
            item.state = ScheduleItem.EXECUTING
            self.schedule.mapping[node].append(item)
            ## TODO: generate id here
            event = NodeFailed()
            event.time_posted = current_time
            event.time_happened = end_time
            event.job = scitem.job
            event.node = node
            event.duration = duration
            self.post_event(event)



        def check_fail(end_time):
            reliability = self.estimator.estimate_reliability(scitem.job, node)
            res = random.random()
            if res > reliability:
                return True
            return False

        comp_time = self.estimator.estimate_runtime(scitem.job, node)
        end_time = current_time + comp_time

        if check_fail(end_time):
            failed_run(end_time)
        else:
            normal_run(end_time)

    def generate_nodeup(self, node_failed_event, current_time):
        nodeup = NodeUp()
        nodeup.node = node_failed_event.node
        nodeup.time_posted = current_time
        nodeup.time_happened = current_time + node_failed_event.duration
        nodeup.failed_task = node_failed_event.task
        self.post_event(nodeup)

    pass

## validation
## 1. std validate schedule generated by GAExecutor (modify for accounting failed tasks)
## 2. get intervals of unavailability and check accordance with the generated schedule

