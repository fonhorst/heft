from collections import deque
import random
from GA.DEAPGA.GAImplementation.GAImpl import GAFactory
from core.CommonComponents.failers.FailOnce import FailOnce
from core.comparisons.ComparisonBase import ResultSaver
from core.examples.ExecutorRunner import ExecutorRunner
from core.executors.BaseExecutor import BaseExecutor
from core.executors.EventMachine import TaskFinished, NodeFailed, NodeUp
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem
from environment.Utility import Utility


class GaOldPopExecutor(FailOnce, BaseExecutor):

    def __init__(self,
                 workflow,
                 resource_manager,
                 estimator,
                 ga_params,
                 base_fail_duration,
                 base_fail_dispersion,
                 wf_name,
                 stat_saver,
                 task_id_to_fail,
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

        self.wf_name = wf_name
        self.stat_saver = stat_saver
        self.task_id_to_fail = task_id_to_fail

        self.logger = logger


        self.past_pop = None
        pass

    def init(self):
        ga_planner = self._create_ga()
        self.current_schedule = Schedule({node: [] for node in self.resource_manager.get_nodes()})
        (result, logbook) = ga_planner(self.current_schedule, None)
        self.past_pop = ga_planner.get_pop()
        print("Result makespan: " + str(Utility.get_the_last_time(result[2])))
        self.current_schedule = result[2]
        self._post_new_events()
        self.failed_once = False
        pass

    def _create_ga(self):
        ga_planner = GAFactory.default().create_ga(silent=True,
                                                   wf=self.workflow,
                                                   resource_manager=self.resource_manager,
                                                   estimator=self.estimator,
                                                   ga_params=self.params)
        return ga_planner

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

    def _clean_chromosome(self, chromosome, event, current_cleaned_schedule):

        not_scheduled_tasks = [ item.job.id for (node, items) in current_cleaned_schedule.mapping.items() for item in items if item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.EXECUTING]

        for (node_name, ids) in chromosome.items():
            for_removing = []
            for id in ids:
                if id in not_scheduled_tasks:
                    for_removing.append(id)
                pass
            for r in for_removing:
                ids.remove(r)
                pass
            pass

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

        task_id = "" if not hasattr(event, 'task') else " " + str(event.task.id)
        ## scheduling with initial population created of the previous population by moving elements from a downed node
        print("Scheduling with the old pop: " + str(event.__class__.__name__) + task_id )
        ga_planner = self._create_ga()

        cleaned_chromosomes = [self._clean_chromosome(ch, event, current_cleaned_schedule) for ch in self.past_pop]
        def is_empty(ch):
            return len([item for n, items in ch.items() for item in items]) == 0
        cleaned_chromosomes = [ch for ch in cleaned_chromosomes if not is_empty(ch)]
        cleaned_chromosomes = None if len(cleaned_chromosomes) == 0 else cleaned_chromosomes

        if self.workflow.get_all_unique_tasks() in current_cleaned_schedule.get_all_unique_tasks_id():
            print("Schedule alleady has all unique tasks")
            return

        ((v1, v2, resulted_schedule, iter_old_pop), logbook_old_pop) = ga_planner(current_cleaned_schedule, None, self.current_time, initial_population=cleaned_chromosomes)
        #checking
        Utility.check_and_raise_for_fixed_part(resulted_schedule, current_cleaned_schedule, self.current_time)
        makespan_old_pop = Utility.get_the_last_time(resulted_schedule)
        print("Result makespan: " + str(makespan_old_pop))



        self.current_schedule = resulted_schedule
        self.past_pop = ga_planner.get_pop()

        ## scheduling with random initial population
        print("Scheduling with a random pop: " + str(event.__class__.__name__)+ task_id)
        ga_planner_with_random_init_population = self._create_ga()
        ((v3, v4, schedule_with_random, iter_random), logbook_random) = ga_planner_with_random_init_population(current_cleaned_schedule, None, self.current_time, initial_population=None)

        Utility.check_and_raise_for_fixed_part(schedule_with_random, current_cleaned_schedule, self.current_time)
        makespan_random = Utility.get_the_last_time(schedule_with_random)

        print("Result makespan: " + str(Utility.get_the_last_time(schedule_with_random)))


        # creating and writing some stat data
        # Note: it can be rewritten with using of events
        if self.stat_saver is not None:
            stat_data = {
                "wf_name": self.wf_name,
                "event_name": event.__class__.__name__,
                "task_id": task_id,
                "with_old_pop": {
                    "iter": iter_old_pop,
                    "makespan": makespan_old_pop,
                    "pop_aggr": logbook_old_pop
                },
                "with_random": {
                    "iter": iter_random,
                    "makespan": makespan_random,
                    "pop_aggr": logbook_random
                }
            }
            self.stat_saver(stat_data)


        self._post_new_events()
        pass

    pass


class GaOldPopExecutorRunner(ExecutorRunner):

    DEFAULT_SAVE_PATH = "../../results/GaRescheduleResults_{0}.json"

    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, ga_params, key_for_save, task_id_to_fail=None, the_bundle=None, logger=None):
        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)

        stat_saver = ResultSaver(GaOldPopExecutorRunner.DEFAULT_SAVE_PATH.format(key_for_save))

        ga_machine = GaOldPopExecutor(
                            workflow=wf,
                            resource_manager=resource_manager,
                            estimator=estimator,
                            ga_params=ga_params,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            wf_name=wf_name,
                            stat_saver=stat_saver,
                            task_id_to_fail=task_id_to_fail,
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



