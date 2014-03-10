from collections import deque
import random
from GA.DEAPGA.GAImplementation.GAImplementation import construct_ga_alg
from core.examples.BaseExecutorExample import BaseExecutorExample
from core.executors.BaseSchedulingExecutor import BaseSchedulingExecutor
from core.executors.EventMachine import EventMachine
from core.executors.EventMachine import TaskStart, TaskFinished, NodeFailed, NodeUp
from core.executors.GaHeftExecutor import GAComputationManager
from environment.Resource import Node
from environment.ResourceManager import Schedule, ScheduleItem
from environment.Utility import Utility




class GaExecutor(BaseSchedulingExecutor):

    def __init__(self,
                 name,
                 workflow,
                 resource_manager,
                 estimator,
                 base_fail_duration,
                 base_fail_dispersion,
                 ga_params,
                 stat_saver,
                 logger=None):

        super.__init__(  name,
                         workflow,
                         resource_manager,
                         estimator,
                         base_fail_duration,
                         base_fail_dispersion,
                         logger)

        self.params = ga_params
        self.stat_saver = stat_saver

        self.past_pop = None
        pass

    def init(self):
        ga_planner = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=self.params)
        self.current_schedule = Schedule({node: [] for node in self.resource_manager.get_nodes()})

        (best, pop, schedule) = ga_planner(self.current_schedule, None)
        self.past_pop = ga_planner.get_pop()
        self.current_schedule = schedule
        self._post_new_events()
        self.failed_once = False
        pass

    def _check_fail(self, task, node):
        if self.failed_once is not True:
            reliability = self.estimator.estimate_reliability(task, node)
            res = random.random()
            if res > reliability or task.id == "ID00015_000":
                self.failed_once = True
                return True
        return False

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

        ## scheduling with initial population created of the previous population by moving elements from a downed node
        print("Scheduling with the old pop: " + str(event.__class__.__name__))
        ga_planner = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=self.params)
        cleaned_chromosomes = [self._clean_chromosome(ch, event, current_cleaned_schedule) for ch in self.past_pop]

        resulted_schedule = ga_planner(current_cleaned_schedule, None, self.current_time, initial_population=cleaned_chromosomes)[2]
        #checking
        Utility.check_and_raise_for_fixed_part(resulted_schedule, current_cleaned_schedule, self.current_time)

        self.current_schedule = resulted_schedule
        self.past_pop = ga_planner.get_pop()

        ## scheduling with random initial population
        print("Scheduling with a random pop: " + str(event.__class__.__name__))
        ga_planner_with_random_init_population = construct_ga_alg(True,
                                       self.workflow,
                                       self.resource_manager,
                                       self.estimator,
                                       params=self.params)
        schedule_with_random = ga_planner_with_random_init_population(current_cleaned_schedule, None, self.current_time, initial_population=None)[2]

        Utility.check_and_raise_for_fixed_part(schedule_with_random, current_cleaned_schedule, self.current_time)

        self._post_new_events()
        pass
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



