from collections import deque
from heft.core.environment.BaseElements import Node

from heft.core.environment.EventMachine import TaskStart, TaskFinished, NodeFailed, NodeUp
from heft.core.environment.EventMachine import EventMachine
from heft.core.environment.ResourceManager import ScheduleItem, Schedule
from heft.utilities.common import trace


class BaseExecutor(EventMachine):
    #@trace
    def __init__(self, *args, **kwargs):
        super().__init__()

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
        raise Exception("Unknown event: " + str(event))

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
        self.queue = deque([event for event in self.queue if check(event)])
        return clean_schedule

    def _remove_events(self, check_func):
        self.queue = deque([ev for ev in self.queue if check_func(ev)])
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

    def _reschedule(self, event):
        self.heft_planner.current_time = self.current_time
        current_cleaned_schedule = self._clean_events(event)

        if len([nd for nd in self.resource_manager.get_nodes() if nd.state != Node.Down]) == 0:
            return

        self.current_schedule = self.heft_planner.run(current_cleaned_schedule)

        ## TODO: remove it later.
        # makespan = Utility.makespan(self.current_schedule)
        # id = "{0} {1}".format(event.task.id, event.node.flops) if isinstance(event, NodeFailed) else "?"
        # print("RESCHEDULE EVENT: {0}({1}) HEFT MAKESPAN: {2:.2f}".format(event.__class__.__name__, id, makespan))

        self._post_new_events()

    def _task_start_handler(self, event):
        pass

    def _task_finished_handler(self, event):
        pass

    def _node_failed_handler(self, event):
        pass

    def _node_up_handler(self, event):
        pass

    pass
