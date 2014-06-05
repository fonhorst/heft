##interface Algorithm


class Algorithm:
    def __init__(self):
        self.resource_manager = None
        self.estimator = None

    def run(self, event):
        pass

##interface ResourceManager
class ResourceManager:
    def __init__(self):
        pass

    ##get all resources in the system
    def get_resources(self):
        pass

    def change_performance(self, node, performance):
        pass

    ## TODO: remove duplcate code with HeftHelper
    def get_nodes(self):
        resources = self.get_resources()
        result = set()
        for resource in resources:
            result.update(resource.nodes)
        return result

##interface Estimator
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

## element of Schedule
class ScheduleItem:
    UNSTARTED = "unstarted"
    FINISHED = "finished"
    EXECUTING = "executing"
    FAILED = "failed"
    def __init__(self, job, start_time, end_time):
        self.job = job ## either task or service operation like vm up
        self.start_time = start_time
        self.end_time = end_time
        self.state = ScheduleItem.UNSTARTED

    @staticmethod
    def copy(item):
        new_item = ScheduleItem(item.job, item.start_time, item.end_time)
        new_item.state = item.state
        return new_item

    @staticmethod
    def MIN_ITEM():
        return ScheduleItem(None, 10000000, 10000000)

    def __str__(self):
        return str(self.job.id) + ":" + str(self.start_time) + ":" + str(self.end_time) + ":" + self.state

    def __repr__(self):
        return str(self.job.id) + ":" + str(self.start_time) + ":" + str(self.end_time) + ":" + self.state


class Schedule:
    def __init__(self, mapping):
        ## {
        ##   res1: (task1,start_time1, end_time1),(task2,start_time2, end_time2), ...
        ##   ...
        ## }
        self.mapping = mapping##dict()

    def is_finished(self, task):
        (node, item) = self.place(task)
        if item is None:
            return False
        return item.state == ScheduleItem.FINISHED

    def get_next_item(self, task):
        for (node, items) in self.mapping.items():
            l = len(items)
            for i in range(l):
                if items[i].job.id == task.id:
                    if l > i + 1:
                        return items[i + 1]
                    else:
                        return None
        return None

    def place(self, task):
        for (node, items) in self.mapping.items():
            for item in items:
                if item.job.id == task.id:
                    return (node,item)
        return None

    def change_state_executed(self, task, state):
        for (node, items) in self.mapping.items():
            for item in items:
                if item.job.id == task.id and (item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.UNSTARTED):
                    item.state = state
        return None

    def place_single(self, task):
        for (node, items) in self.mapping.items():
            for item in items:
                if item.job.id == task.id and (item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.UNSTARTED):
                    return (node, item)
        return None

    def change_state_executed_with_end_time(self, task, state, time):
        for (node, items) in self.mapping.items():
            for item in items:
                if item.job.id == task.id and item.state == ScheduleItem.EXECUTING:
                    item.state = state
                    item.end_time = time
                    return True
        #print("gotcha_failed_unstarted task: " + str(task))
        return False

    def place_by_time(self, task, start_time):
        for (node, items) in self.mapping.items():
            for item in items:
                if item.job.id == task.id and item.start_time == start_time:
                    return (node,item)
        return None

    def is_executing(self, task):
        for (node, items) in self.mapping.items():
            for item in items:
                if item.job.id == task.id and item.state == ScheduleItem.EXECUTING:
                    return True
        return False


    def change_state(self, task, state):
        (node, item) = self.place(task)
        item.state = state

    # def get_all_unique_tasks_id(self):
    #     ids = set(item.job.id for (node, items) in self.mapping.items() for item in items)
    #     return ids

    def get_all_unique_tasks(self):
        tasks = set(item.job for (node, items) in self.mapping.items() for item in items)
        return tasks

    def get_all_unique_tasks_id(self):
        tasks = self.get_all_unique_tasks()
        ids = set(t.id for t in tasks)
        return ids

    def task_to_node(self):
        """
        This operation is applicable only for static scheduling.
        i.e. it is assumed that each is "executed" only once and only on one node.
        Also, all tasks must have state "Unstarted".
        """
        all_items = [item for node, items in self.mapping.items() for item in items]
        assert all(it.state == ScheduleItem.UNSTARTED for it in all_items),\
            "This operation is applicable only for static scheduling"
        t_to_n = {item.job.id: node for (node, items) in self.mapping.items() for item in items}
        return t_to_n



    @staticmethod
    def insert_item(mapping, node, item):
        result = []
        i = 0
        while i < len(mapping[node]):
            ## TODO: potential problem with double comparing
            if mapping[node][i].start_time >= item.end_time:
                break
            i += 1
        mapping[node].insert(i, item)


    def get_items_in_time(self, time):
        pass

    ## gets schedule consisting of only currently running tasks
    def get_schedule_in_time(self, time):
        pass

    def get_the_most_upcoming_item(self,time):
        pass

##interface Executor
#class Executor:
#    def __init__(self):
#        ##self.schedule = None
#        self.algorithm = None
#        self.resource_manager = None
#        self.estimator = None
#
#        self.posting_entity = None
#
#
#    ## check if possible to interrupt execution of the current executing task on the node
#    def can_interrupt_execution(self, task, node):
#        pass
#
#    def event_arrived(self, event):
#        # if self.need_to_reschedule(event):
#        #     self.schedule = self.get_scheduler()
#        #
#        # self.clean_queue()
#        # self.generate_events()
#        schedule = self.algorithm.run(event)
#        if schedule is not None:
#            ##TODO: generate appropriate events
#            # select for executing and generate appropriate events
#            self.post_event(new_event)
#
#
#    def post_event(self, event):
#        self.posting_entity.post(event)
#
#    def need_to_reschedule(self, event):
#        pass
#
#    def get_scheduler(self):
#        pass
#
#    def clean_queue(self):
#        pass
#
#    def generate_events(self):
#        pass

##interface Scheduler
class Scheduler:
    def __init__(self):
        ##previously built schedule
        self.old_schedule = None
        self.resource_manager = None
        self.estimator = None
        self.executor = None
        self.workflows = None

    ## build and returns new schedule
    def schedule(self):
        pass
