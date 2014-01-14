__author__ = 'nikolay'


class ResourceManager:
    def __init__(self):
        pass

    ##get all resources in the system
    def get_resources(self):
        pass


class Estimator:
    def __init__(self):
        pass

    ##get estimated time of running the task on the node
    def estimate_runtime(self, task, node):
        pass

    ## estimate transfer time between node1 and node2 for data generated by the task
    def estimate_transfer_time(self, node1, node2, task1, task2):
        pass

    ## estimate probability of successful ending of the task on the node
    def estimate_reliability(self, task, node):
        pass


class ScheduleItem:
    def __init__(self, job, start_time, end_time):
        self.job = job ## either task or service operation like vm up
        self.start_time = start_time
        self.end_time = end_time


class Schedule:
    def __init__(self, mapping):
        ## {
        ##   res1: (task1,start_time1, end_time1),(task2,start_time2, end_time2), ...
        ##   ...
        ## }
        self.mapping = mapping##dict()

    def get_items_in_time(self, time):
        pass

    ## gets schedule consisting of only currently running tasks
    def get_schedule_in_time(self, time):
        pass

    def get_the_most_upcoming_item(self,time):
        pass


class Executor:
    def __init__(self):
        self.schedule = None

    ## check if possible to interrupt execution of the current executing task on the node
    def can_interrupt_execution(self, task, node):
        pass

    def event_arrived(self, event):
        if self.need_to_reschedule(event):
            self.schedule = self.get_scheduler()

        self.clean_queue()
        self.generate_events()

    def need_to_reschedule(self, event):
        pass

    def get_scheduler(self):
        pass

    def clean_queue(self):
        pass

    def generate_events(self):
        pass





class Scheduler:
    def __init__(self):
        ##previously built schedule
        self.old_schedule = None
        self.resource_manager = None
        self.estimator = None
        self.event = None
        self.executor = None
        self.workflows = None

    ## build and returns new schedule
    def build_schedule(self):
        pass
