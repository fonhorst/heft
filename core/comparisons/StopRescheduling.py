from collections import deque
import random
from GA.DEAPGA.GAImplementation.GAImplementation import construct_ga_alg
from core.examples.BaseExecutorExample import BaseExecutorExample
from core.executors.EventMachine import EventMachine
from core.executors.EventMachine import TaskStart, TaskFinished, NodeFailed, NodeUp
from core.executors.GaHeftExecutor import GAComputationManager
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem


class GaExecutor(EventMachine):

    def __init__(self,
                 workflow,
                 resource_manager,
                 estimator,
                 ga_params,
                 base_fail_duration,
                 base_fail_dispersion,

                 entity_name,
                 stat_saver,

                 logger=None):
        self.queue = deque()
        self.current_time = 0

        self.base_fail_duration = base_fail_duration
        self.base_fail_dispersion = base_fail_dispersion
        self.current_schedule = None


        self.workflow = workflow
        self.resource_manager = resource_manager
        self.estimator = estimator
        self.params = ga_params

        self.entity_name = entity_name
        self.stat_saver = stat_saver

        self.logger = logger


        self.past_pop = None
        pass

    def init(self):
        ga_planner = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=self.params)
        self.current_schedule = Schedule({node: [] for node in self.resource_manager.get_nodes()})

        result = ga_planner(self.current_schedule, None)
        self.past_pop = ga_planner.get_pop()
        self.current_schedule = result[2]
        self._post_new_events()

        self.failed_once = False
        pass

    #def _check_fail(self, task, node):
    #    reliability = self.estimator.estimate_reliability(task, node)
    #    res = random.random()
    #    if res > reliability:
    #        return True
    #    return False

    def _check_fail(self, task, node):
        if self.failed_once is not True:
            reliability = self.estimator.estimate_reliability(task, node)
            res = random.random()
            if res > reliability or task.id == "ID00015_000":
                self.failed_once = True
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
            # remove TaskFinished event
            ##TODO: make a function for this purpose in the base class
            self.queue = deque([ev for ev in self.queue if not (isinstance(ev, TaskFinished) and ev.task.id == event.task.id)])
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

    def _clean_chromosome(self, chromosome, event):
            if isinstance(event, NodeFailed):
                tasks = chromosome[event.node.name]
                ## TODO: here must be a procedure of getting currently alive nodes
                working_nodes = list(chromosome.keys() - set([event.node.name]))
                for t in tasks:
                    lt = len(working_nodes) - 1
                    new_node = 0 if lt == 0 else random.randint(0, lt )
                    node_name = working_nodes[new_node]
                    length = len(chromosome[node_name])
                    # TODO: correct 0 and length
                    new_place = 0 if length == 0 else random.randint(0, length)
                    chromosome[node_name].insert(new_place, t)
                chromosome[event.node.name] = []
                return chromosome
            if isinstance(event, NodeUp):
                pass
            return chromosome

    def _reschedule(self, event):
        current_cleaned_schedule = self._clean_events(event)

        ## scheduling with initial population created of the previous population by moving elements from a downed node
        print("Scheduling with the old pop: " + str(event.__class__.__name__))
        ga_planner = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=self.params)
        cleaned_chromosomes = [self._clean_chromosome(ch, event) for ch in self.past_pop]
        self.current_schedule = ga_planner(current_cleaned_schedule, None, self.current_time, initial_population=cleaned_chromosomes)[2]
        self.past_pop = ga_planner.get_pop()

        ## scheduling with random initial population
        print("Scheduling with a random pop: " + str(event.__class__.__name__))
        ga_planner_with_random_init_population = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=self.params)
        schedule_with_random = ga_planner_with_random_init_population(current_cleaned_schedule, None, self.current_time, initial_population=None)

        self._post_new_events()
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
        self.queue = deque([event for event in self.queue if check(event)])
        return clean_schedule

    pass


class GaExecutorExample(BaseExecutorExample):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, ga_params, the_bundle=None, logger=None):
        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)

        ga_machine = GaExecutor(
                            workflow=wf,
                            resource_manager=resource_manager,
                            estimator=estimator,
                            ga_params=ga_params,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            entity_name=None,
                            stat_saver=None,
                            logger=logger)

        ga_machine.init()
        ga_machine.run()

        resulted_schedule = ga_machine.current_schedule
        (makespan, time_seq, depend_seq) = self.extract_result(resulted_schedule, is_silent, wf)

        if time_seq is not True:
            raise Exception("Time sequence isn't valid")
        if depend_seq is not True:
            raise Exception("Dependency sequence isn't valid")
        return makespan

    pass



