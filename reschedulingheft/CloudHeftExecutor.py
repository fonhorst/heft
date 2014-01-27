from collections import deque
import random
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem
from reschedulingheft.HeftExecutor import EventMachine, TaskStart, NodeFailed, NodeUp, TaskFinished


class CloudHeftExecutor(EventMachine):


    def __init__(self, heft_planner, base_fail_duration, base_fail_dispersion, desired_reliability):
        ## TODO: remake it later
        self.queue = deque()
        self.current_time = 0
        # DynamicHeft
        self.heft_planner = heft_planner
        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        self.desired_reliability = desired_reliability
        self.current_schedule = Schedule({node:[] for node in heft_planner.get_nodes()})

    def init(self):
        self.current_schedule = self.heft_planner.run(self.current_schedule)
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

            # public_resources_manager:
            #   determine nodes of proper soft type
            #   check and determine free nodes
            #   determine reliability of every nodes
            #   determine time_of_execution probability for (task,node) pair

            # try to find nodes in cloud
            proper_nodes = public_resources_manager.get_nodes_by_type()
            proper_nodes = get_free_nodes(proper_nodes)
            sorted_proper_nodes = sorted(proper_nodes, key=lambda x: x reliability)
            current_set = [event.node]
            for pnode in sorted_proper_nodes:
                current_set.append(pnode)
                common_reliability = calculate_set_reliability(current_set)
                if common_reliability >= desired_reliability:
                    break
            current_set.remove(event.node)
            for nd in current_set:
                generate_fail_or_success
                    True: generate_time_of_fail
                    False: generate_time_of_execution
                post_events ## TaskStart + TaskFinished or TaskStart + NodeFailed
            register_task ## register for multiple start





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

            # check if it cloud task
            # if task cloud and first: register as finished, check node in dedicated as finish, remove appropriate event of failure or task finished for dedicated, free cloud node, reschedule, end_of_function
            # if task cloud and not first: free cloud node, end_of_function
            # if task not cloud and first: register as finished, check node in dedicated as finish, end_of_function

            # check task finished
            self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)
            return None
        if isinstance(event, NodeFailed):

            # check if cloud node
            # if cloud node: check as down, end_of_function
            # if not cloud node: check as down, reschedule, register new node in dedicated resource for task if changed, end_of_function


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

            # check if cloud
            # if cloud: check node up, end_of_function
            # if not cloud: check as up, reschedule, register new node in dedicated resource for task if changed, end_of_function

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











