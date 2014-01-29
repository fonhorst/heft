from collections import deque
import random
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem
from reschedulingheft.HeftExecutor import EventMachine, TaskStart, NodeFailed, NodeUp, TaskFinished


class CloudHeftExecutor(EventMachine):

    STATUS_RUNNING = 'running'
    STATUS_FINISHED = 'finished'

    def __init__(self, heft_planner, base_fail_duration, base_fail_dispersion, desired_reliability, public_resource_manager):
        ## TODO: remake it later
        self.queue = deque()
        self.current_time = 0
        # DynamicHeft
        self.heft_planner = heft_planner
        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        self.desired_reliability = desired_reliability
        self.public_resources_manager = public_resource_manager
        self.current_schedule = Schedule({node: [] for node in heft_planner.get_nodes()})

        self.register = dict()

    def init(self):
        self.current_schedule = self.heft_planner.run(self.current_schedule)
        self.post_new_events()

    def event_arrived(self, event):

        def reschedule(event):
            self.heft_planner.current_time = self.current_time
            current_cleaned_schedule = self.clean_events(event)
            self.current_schedule = self.heft_planner.run(current_cleaned_schedule)
            self.post_new_events()

        def check_fail(reliability):
            res = random.random()
            if res > reliability:
                return True
            return False


        if isinstance(event, TaskStart):

            # TODO: if node is cloud node, do nothing
            prm = self.public_resources_manager
            if prm.isCloudNode(event.node):
                return None

            # check if failed and post
            (node, item) = self.current_schedule.place_by_time(event.task, event.time_happened)
            item.state = ScheduleItem.EXECUTING

            # check task as executing
            # self.current_schedule.change_state(event.task, ScheduleItem.EXECUTING)

            # public_resources_manager:
            #   determine nodes of proper soft type
            #   check and determine free nodes
            #   determine reliability of every nodes
            #   determine time_of_execution probability for (task,node) pair

            # try to find nodes in cloud

            if event.task not in self.register:
                proper_nodes = prm.get_by_softreq(event.task.soft_reqs)
                proper_nodes = [node for node in proper_nodes if not prm.isBusy(node)]
                sorted_proper_nodes = sorted(proper_nodes, key=lambda x: prm.get_reliability(x.name))
                current_set = []

                base_reliability = self.heft_planner.estimator.estimate_reliability(event.task, event.node)
                obtained_reliability = base_reliability
                dt = item.end_time - item.start_time
                def calc(node, dt):
                        #(dt, task, node, transfer_estimation)
                        # TODO: add proper transfer time here
                        fp = prm.get_reliability(node.name)
                        comp_time = self.heft_planner.estimator.estimate_runtime(event.task, node)
                        cp = prm.probability_estimator(dt, comp_time, 0)
                        return (node, fp, cp )

                for pnode in sorted_proper_nodes:
                    current_set.append(calc(pnode, dt))
                    #TODO: add dencity law of probability for dedicated resource
                    common_reliability = 1 - base_reliability
                    for (nd, fp, cp) in current_set:
                        common_reliability *= (1 - fp*cp)
                    common_reliability = 1 - common_reliability

                    if common_reliability >= self.desired_reliability:
                        break
                #print(" Obtained reliability " + str(obtained_reliability) + " for task: " + str(event.task))

                def frange(x, y, jump):
                    while x < y:
                        yield x
                        x += jump

                for (nd, fp, cp) in current_set:
                    comp_time = self.heft_planner.estimator.estimate_runtime(event.task, nd)
                    comp_time = comp_time + 0.6*comp_time
                    ints = [(i, calc(nd, i))for i in frange(0, comp_time, 0.05*comp_time)]
                    rd = random.random()
                    generated_comp_time = comp_time
                    for (i, p) in ints:
                        if p[2] > rd:
                            generated_comp_time = i
                            break

                    if check_fail(fp):
                        duration = self.base_fail_duration + self.base_fail_dispersion *random.random()
                        time_of_fail = generated_comp_time*random.random()
                        time_of_fail = self.current_time + (time_of_fail if time_of_fail > 0 else 0.01) ##(item.end_time - self.current_time)*0.01

                        event_failed = NodeFailed(nd, event.task)
                        event_failed.time_happened = time_of_fail

                        event_nodeup = NodeUp(nd)
                        event_nodeup.time_happened = time_of_fail + duration

                        self.post(event_failed)
                        self.post(event_nodeup)
                    else:
                        event_start = TaskStart(event.task)
                        event_start.time_happened = self.current_time
                        event_start.node = nd

                        event_finish = TaskFinished(event.task)
                        event_finish.time_happened = self.current_time + generated_comp_time
                        event_finish.node = nd

                        self.post(event_start)
                        self.post(event_finish)

                    prm.checkBusy(nd, True)

                self.register[event.task] = CloudHeftExecutor.STATUS_RUNNING
                pass

            reliability = self.heft_planner.estimator.estimate_reliability(event.task, node)
            if check_fail(reliability):
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
            prm = self.public_resources_manager
            from_cloud = prm.isCloudNode(event.node)
            if from_cloud and self.register[event.task] == CloudHeftExecutor.STATUS_RUNNING:
                self.register[event.task] = CloudHeftExecutor.STATUS_FINISHED
                ## TODO: correct it
                ## if event.task failed and went through rescheduling,
                ## it would be possible that currently ScheduleItem of event.task on dedicated resource
                ## has UNSTARTED state.
                ## TODO: add additional functional to schedule to record such situations and validate it after
                self.current_schedule.change_state_executed_with_end_time(event.task, ScheduleItem.FINISHED, self.current_time)
                def check(ev):
                    if isinstance(ev, TaskFinished) or isinstance(ev, NodeFailed):
                        if ev.task.id == event.task.id:
                            return False
                    ## TODO: make it later
                    ##if isinstance(ev, NodeUp):
                    return True
                self.queue = [ev for ev in self.queue if check(ev)]
                prm.checkBusy(event.node, False)
                reschedule(event)
                return None
            if from_cloud and self.register[event.task] == CloudHeftExecutor.STATUS_FINISHED:
                prm.checkBusy(event.node, False)
                return None

            # check task finished
            self.register[event.task] = CloudHeftExecutor.STATUS_FINISHED
            self.current_schedule.change_state_executed(event.task, ScheduleItem.FINISHED)
            return None
        if isinstance(event, NodeFailed):

            # check if cloud node
            # if cloud node: check as down, free node, end_of_function
            # if not cloud node: check as down, reschedule, end_of_function
            prm = self.public_resources_manager
            from_cloud = prm.isCloudNode(event.node)

            if from_cloud:
                prm.checkDown(event.node.name, True)
                prm.checkBusy(event.node, False)
                return None


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
            # if not cloud: check as up, reschedule end_of_function
            prm = self.public_resources_manager
            from_cloud = prm.isCloudNode(event.node)
            if from_cloud:
                prm.checkDown(event.node.name, False)
                return None


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
                    unstarted_items.add((node, item))

        events_to_post = []
        for (node, item) in unstarted_items:
            event_start = TaskStart(item.job)
            event_start.time_happened = item.start_time
            event_start.node = node

            event_finish = TaskFinished(item.job)
            event_finish.time_happened = item.end_time
            event_finish.node = node

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











