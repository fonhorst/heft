import random
from environment.ResourceManager import ScheduleItem, Schedule
from environment.Utility import Utility

##TODO: obsolete, check code and remove it later
class GAFunctions:
    def __init__(self,
                     workflow,
                     nodes,
                     sorted_tasks,
                     estimator,
                     size):

            self.counter = 0
            self.workflow = workflow
            self.nodes = nodes
            self.sorted_tasks = sorted_tasks
            self.workflow_size = len(sorted_tasks)

            ##interface Estimator

            self.estimator = estimator
            self.size = size

            self.task_map = {task.id: task for task in sorted_tasks}
            self.node_map = {node.name: node for node in nodes}

            self.initial_chromosome = None##GAFunctions.schedule_to_chromosome(initial_schedule)
            pass

    @staticmethod
    def schedule_to_chromosome(schedule, sorted_tasks):
        chrome = []
        for task in sorted_tasks:
            (node, item) = schedule.place(task)
            chrome.append((task.id, node.name))
        return chrome

    def initial(self):
        ## TODO: remove it later.
        #res = random.random()
        # # TODO:
        #if res >0.8:
        #     return self.initial_chromosome
        ##return [self.random_chromo() for j in range(self.size)]
        return self.random_chromo()

    def crossover(self, child1, child2):
        i1 = random.randint(0, self.workflow_size - 1)
        i2 = random.randint(0, self.workflow_size - 1)
        index1 = min(i1, i2)
        index2 = max(i1, i2)

        for_choosing = [i for i in range(self.workflow_size)]
        swap_map = []
        for i in range(index1, index2):
            rd = random.randint(0, len(for_choosing) - 1)
            choosed = for_choosing[rd]
            swap_map.append((i, choosed))
            for_choosing.remove(choosed)

            ##[(t1, n2) for ((t1, n1), (t2, n2)) in zip(child1[index1:index2], swap_map)]

        new_1 = child1[0:index1] + [(t1, child2[n2][1]) for ((t1, n1), (t2, n2)) in zip(child1[index1:index2], swap_map)] + child1[index2:]##([] if index2 + 1 == len(for_choosing) else child1[index2+1:])
        new_2 = list(child2)
        for (i, choosed) in swap_map:
            new_2[choosed] = (new_2[choosed][0], child1[i][1])

        for i in range(self.workflow_size):
            child1[i] = new_1[i]
            child2[i] = new_2[i]

        ##return (new_1, new_2)




    def fitness(self, chromo):
        ## value of fitness function is the last time point in the schedule
        ## built from the chromo
        ## chromo is {Task:Node},{Task:Node},... - fixed length
        schedule = self.build_schedule(chromo)
        time = Utility.get_the_last_time(schedule)
        return (1/time,)

    def build_schedule(self, chromo):
        ## {
        ##   res1: (task1,start_time1, end_time1),(task2,start_time2, end_time2), ...
        ##   ...
        ## }
        schedule_mapping = dict()
        chrmo_mapping = {task_id: self.node_map[node_name] for (task_id, node_name) in chromo}
        task_to_node = dict()
        estimate = self.estimator.estimate_transfer_time

        # TODO: remove it later
        # def max_parent_finish(task):
        #     ##TODO: remake this stub later
        #     ##TODO: (self.workflow.head_task == list(task.parents)[0]) - False. It's a bug.
        #     ## fix it later.
        #     if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
        #         return 0
        #     return max([task_to_node[p.id][2] for p in task.parents])
        #
        # def transfer(task, node):
        #     ## find all parent nodes mapping
        #     ## estimate with estimator transfer time for it
        #     ##TODO: remake this stub later.
        #     if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
        #         return 0
        #     lst = [estimate(node, chrmo_mapping[parent.id], task, parent) for parent in task.parents]
        #     transfer_time = max(lst)
        #     return transfer_time

        def comm_ready_func(task, node):
            ##TODO: remake this stub later.
            if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
                return 0
            return max([task_to_node[p.id][2]+ estimate(node, chrmo_mapping[p.id], task, p) for p in task.parents])


        def get_possible_execution_time(task, node):
            node_schedule = schedule_mapping.get(node, list())
            free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
            ##data_transfer_time added here

            comm_ready = comm_ready_func(task, node)

            ##st_time = max(free_time, max_parent_finish(task)) + transfer(task, node)
            st_time = max(free_time, comm_ready)
            ed_time = st_time + self.estimator.estimate_runtime(task, node)
            return st_time, ed_time

        for (task_id, node_name) in chromo:
            task = self.task_map[task_id]
            node = self.node_map[node_name]
            (start_time, end_time) = get_possible_execution_time(task, node)
            item = ScheduleItem(task, start_time, end_time)
            lst = schedule_mapping.get(node, list())
            lst.append(item)
            schedule_mapping[node] = lst
            task_to_node[task.id] = (node, start_time, end_time)

        schedule = Schedule(schedule_mapping)
        return schedule

    def random_chromo(self):
        nodes_len = len(self.nodes)
        def choose_node():
            index = random.randint(0, nodes_len - 1)
            return self.nodes[index]
        return [(self.sorted_tasks[i].id, choose_node().name) for i in range(self.workflow_size)]

    def mutation(self, chromosome):
        # simply change one node of task mapping
        mutated = list(chromosome)
        #for i in range(3):
        index = random.randint(0, self.workflow_size - 1)
        node_index = random.randint(0, len(self.nodes) - 1)
        (task_id, node_name) = chromosome[index]
        mutated[index] = (task_id, self.nodes[node_index].name)


        # index1 = random.randint(0, self.workflow_size - 1)
        # index2 = random.randint(0, self.workflow_size - 1)
        #
        # buf = mutated[index1]
        # mutated[index1] = mutated[index2]
        # mutated[index2] = buf

        return mutated

    pass
