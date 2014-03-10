from collections import deque
import random
from core.executors.EventMachine import EventMachine, TaskStart, TaskFinished, NodeFailed, NodeUp
from environment.ResourceManager import ScheduleItem, Schedule


class BaseSchedulingExecutor(EventMachine):

    def __init__(self,
                 name,
                 workflow,
                 resource_manager,
                 estimator,
                 base_fail_duration,
                 base_fail_dispersion,
                 logger=None):

        super.__init__(logger)



        self.name = name
        self.workflow = workflow
        self.resource_manager = resource_manager
        self.estimator = estimator

        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        self.current_schedule = None

        self.logger = logger
        pass

    def init(self):
        pass

    def _clean_queue(self, key):
        self.queue = deque(ev for ev in self.queue if key(ev))


    def _check_fail(self, task, node):
        reliability = self.heft_planner.estimator.estimate_reliability(task, node)
        res = random.random()
        if res > reliability:
            return True
        return False


    def event_arrived(self, event):
        if isinstance(event, TaskStart):
            self._task_start_handler(event)
            return
        if isinstance(event, TaskFinished):
            self._task_finished_handler(event)
            return
        if isinstance(event, NodeFailed):
            self._node_failed_handler(event)
            return
        if isinstance(event, NodeUp):
            self._node_up_handler(event)
            return
        pass

    def _task_start_handler(self, event):
        # check task as executing
        # self.current_schedule.change_state(event.task, ScheduleItem.EXECUTING)
        # try to find nodes in cloud
        # check if failed and post
        (node, item) = self.current_schedule.place_by_time(event.task, event.time_happened)
        item.state = ScheduleItem.EXECUTING

        if self._check_fail(event.task, node):
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
            self._clean_queue(key=lambda ev: not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id))

        pass

    def _task_finished_handler(self, event):
        # check task finished
        self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)
        pass

    def _node_failed_handler(self, event):
        self.resource_manager.node(event.node).state = Node.Down
        it = [item for item in self.current_schedule.mapping[event.node] if item.job.id == event.task.id and item.state == ScheduleItem.EXECUTING]
        if len(it) != 1:
            raise Exception("several items founded")
            pass

        it[0].state = ScheduleItem.FAILED
        it[0].end_time = self.current_time

        self._reschedule(event)
        pass

    def _node_up_handler(self, event):
        self.resource_manager.node(event.node).state = Node.Unknown
        self._reschedule(event)
        pass

    def _reschedule(self, event):
        pass

    def _post_new_events(self):
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

    def _clean_events(self, event):

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
        ##TODO: refactor it later
        self._clean_queue(key=check)
        return clean_schedule
    pass
