from GA.GA import GeneticFunctions, GeneticAlgorithm
import random
from environment.ResourceManager import ScheduleItem, Schedule, Scheduler
from reschedulingheft.HeftHelper import HeftHelper


class SchedluerGeneticFunctions(GeneticFunctions):

    def __init__(self,
                     workflow,
                     nodes,
                     sorted_tasks,
                     estimator,
                     limit=200, size=400,
                     prob_crossover=0.9,
                     prob_mutation=0.2):
            self.counter = 0
            self.workflow = workflow
            self.nodes = nodes
            self.sorted_tasks = sorted_tasks
            self.workflow_size = len(sorted_tasks)

            ##interface Estimator
            self.estimator = estimator

            self.limit = limit
            self.size = size
            self.prob_crossover = prob_crossover
            self.prob_mutation = prob_mutation
            pass

    # GeneticFunctions interface impls
    def probability_crossover(self):
        return self.prob_crossover

    def probability_mutation(self):
        return self.prob_mutation

    def initial(self):
        return [self.random_chromo() for j in range(self.size)]

    def fitness(self, chromo):
        ## value of fitness function is the last time point in the schedule
        ## built from the chromo
        ## chromo is {Task:Node},{Task:Node},... - fixed length
        schedule = self.build_schedule(chromo)
        time = self.get_the_last_time(schedule)
        return time

    def get_the_last_time(self, schedule):
        def get_last_time(node_items):
            return 0 if len(node_items) == 0 else node_items[-1].end_time
        last_time = max([get_last_time(node_items) for (node, node_items) in schedule.mapping.items()])
        return last_time

    def build_schedule(self, chromo):
        ## {
        ##   res1: (task1,start_time1, end_time1),(task2,start_time2, end_time2), ...
        ##   ...
        ## }
        schedule_mapping = dict()
        chrmo_mapping = {task: node for (task, node) in chromo}
        task_to_node = dict()
        estimate = self.estimator.estimate_transfer_time

        def max_parent_finish(task):
            ##TODO: remake this stub later
            if len(task.parents) == 1 and self.workflow.head_task == list(task.parents)[0]:
                return 0
            return max([task_to_node[p][2] for p in task.parents])

        def transfer(task, node):
            ## find all parent nodes mapping
            ## estimate with estimator transfer time for it
            ##TODO: remake this stub later.
            if len(task.parents) == 1 and self.workflow.head_task == list(task.parents)[0]:
                return 0
            lst = [estimate(node, chrmo_mapping[parent], task, parent) for parent in task.parents]
            transfer_time = max(lst)
            return transfer_time


        def get_possible_execution_time(task, node):
            node_schedule = schedule_mapping.get(node, list())
            free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
            ##data_transfer_time added here

            st_time = max(free_time, max_parent_finish(task)) + transfer(task, node)
            ed_time = st_time + self.estimator.estimate_runtime(task, node)
            return st_time, ed_time

        for (task, node) in chromo:
            (start_time, end_time) = get_possible_execution_time(task, node)
            item = ScheduleItem(task, start_time, end_time)
            lst = schedule_mapping.get(node, list())
            lst.append(item)
            schedule_mapping[node] = lst
            task_to_node[task] = (node, start_time, end_time)

        schedule = Schedule(schedule_mapping)
        return schedule

    def check_stop(self, fits_populations):
        self.counter += 1
        if self.counter % 10 == 0:
            ##best_match = list(sorted(fits_populations, key=lambda pair: pair[0], reverse=True))[-1][1]
            fits = [f for f, ch in fits_populations]
            best = min(fits)
            worst = max(fits)
            ave = sum(fits) / len(fits)
            print(
                "[G %3d] score=(best: %4d, ave: %4d, worst: %4d)" %
                (self.counter, best, ave, worst))
            pass
        return self.counter >= self.limit

    def parents(self, fits_populations):
        while True:
            father = self.tournament(fits_populations)
            mother = self.tournament(fits_populations)
            yield (father, mother)
            pass
        pass

    def crossover(self, parents):
        father, mother = parents
        index1 = random.randint(1, self.workflow_size - 2)
        index2 = random.randint(1, self.workflow_size - 2)
        if index1 > index2:
            index1, index2 = index2, index1
        child1 = father[:index1] + mother[index1:index2] + father[index2:]
        child2 = mother[:index1] + father[index1:index2] + mother[index2:]
        return child1, child2

    def mutation(self, chromosome):
        ## simply change one node of task mapping
        index = random.randint(0, self.workflow_size - 1)
        node_index = random.randint(0, len(self.nodes) - 1)
        (task, node) = chromosome[index]
        mutated = list(chromosome)
        mutated[index] = (task, self.nodes[node_index])
        return mutated

    # internals
    def tournament(self, fits_populations):
        alicef, alice = self.select_random(fits_populations)
        bobf, bob = self.select_random(fits_populations)
        return alice if alicef < bobf else bob

    def select_random(self, fits_populations):
        return fits_populations[random.randint(0, len(fits_populations)-1)]

    def random_chromo(self):
        nodes_len = len(self.nodes)
        def choose_node():
            index = random.randint(0, nodes_len - 1)
            return self.nodes[index]
        return [(self.sorted_tasks[i], choose_node()) for i in range(self.workflow_size)]

    pass


class StaticGeneticScheduler(Scheduler):
    def __init__(self,
                 estimator,
                 wf,
                 resources,
                 generation_count=200,
                 population_size=400,
                 crossover_probability=0.9,
                 mutation_probability=0.2):
        ##previously built schedule
        self.estimator = estimator
        self.wf = wf
        self.resources = resources
        self.generation_count = generation_count
        self.population_size = population_size
        self.crossover_probability = crossover_probability
        self.mutation_probability = mutation_probability


    ## build and returns new schedule
    def schedule(self):
        nodes = list(HeftHelper.to_nodes(self.resources))

        def compcost(job, agent):
            return self.estimator.estimate_runtime(job, agent)

        def commcost(ni, nj, A, B):
            ##TODO: remake it later
            return 10
            ##return self.estimator.estimate_transfer_time(A, B, ni, nj)

        ranking = HeftHelper.build_ranking_func(nodes, compcost, commcost)
        sorted_tasks = ranking(self.wf)

        print('=================')
        for task in sorted_tasks:
            print(str(task))
        print('=================')

        functions = SchedluerGeneticFunctions(self.wf,
                                              nodes,
                                              sorted_tasks,
                                              self.estimator,
                                              limit=self.generation_count,
                                              size=self.population_size,
                                              prob_crossover=self.crossover_probability,
                                              prob_mutation=self.mutation_probability)
        ga = GeneticAlgorithm(functions)

        bestSolution = ga.run()
        schedule = functions.build_schedule(bestSolution)

        return schedule

    pass
