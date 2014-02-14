## from Buyya
import random
from GA.DEAPGA.GAImplementation.ScheduleBuilder import ScheduleBuilder
from GA.DEAPGA.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic
from environment.Resource import Node
from environment.ResourceManager import ScheduleItem, Schedule
from environment.Utility import Utility

def mark_finished(schedule):
    for (node, items) in schedule.mapping.items():
        for item in items:
            item.state = ScheduleItem.FINISHED

class GAFunctions2:
    ## A chromosome representation
    ##{
    ## node_name: task1.id, task2.id, ... #(order of tasks is important)
    ## ...
    ##}
    def __init__(self,
                     workflow,
                     nodes,
                     sorted_tasks,
                     resource_manager,
                     estimator,
                     size):

            self.counter = 0
            self.workflow = workflow
            self.nodes = nodes
            self.sorted_tasks = sorted_tasks
            self.workflow_size = len(sorted_tasks)

            ##interface Estimator

            self.estimator = estimator
            self.resource_manager = resource_manager
            self.size = size

            self.task_map = {task.id: task for task in sorted_tasks}
            self.node_map = {node.name: node for node in nodes}

            self.initializing_alg = SimpleRandomizedHeuristic(self.workflow, self.nodes, self.estimator)

            self.initial_chromosome = None##GAFunctions.schedule_to_chromosome(initial_schedule)
            pass

    @staticmethod
    def schedule_to_chromosome(schedule):
        if schedule is None:
            return None
        def ids(items):
            return [item.job.id for item in items]
        chromosome = {node.name: ids(items) for (node, items) in schedule.mapping.items()}
        return chromosome

    def build_initial(self, fixed_schedule_part, current_time):
        def initial():
            return self.random_chromo(fixed_schedule_part, current_time)
        return initial

    def random_chromo(self, fixed_schedule_part, current_time):
        print("random_chromo start")
        res = random.random()
        # # TODO:
        if res > 0.8 and self.initial_chromosome is not None:
            return self.initial_chromosome
        ##return [self.random_chromo() for j in range(self.size)]
        sched = self.initializing_alg.schedule(fixed_schedule_part, current_time)
        #TODO: remove it later
        # mark_finished(sched)
        # seq_time_validaty = Utility.validateNodesSeq(sched)
        # dependency_validaty = Utility.validateParentsAndChildren(sched, self.workflow)

        chromo = GAFunctions2.schedule_to_chromosome(sched)
        if fixed_schedule_part is not None:
            # remove fixed_schedule_part from chromosome
            def is_last_version_of_task_executing(item):
                return item.state == ScheduleItem.EXECUTING or item.state == ScheduleItem.FINISHED or item.state == ScheduleItem.UNSTARTED
            # there is only finished, executing and unstarted tasks planned by Heft, perhaps it should have been called unplanned tasks
            finished_tasks = [item.job.id for (node, items) in fixed_schedule_part.mapping.items() for item in items if is_last_version_of_task_executing(item)]

            # TODO: make common utility function with ScheduleBuilder and SimpleRandomizedHeuristic
            chromo = {node_name: [id for id in ids if not (id in finished_tasks)] for (node_name, ids) in chromo.items()}
        print("random_chromo end")
        return chromo

    def build_fitness(self, fixed_schedule_part, current_time):
        builder = ScheduleBuilder(self.workflow, self.resource_manager, self.estimator, self.task_map, self.node_map, fixed_schedule_part)

        def fitness(chromo):

            print(" fitness start")
            ## value of fitness function is the last time point in the schedule
            ## built from the chromo
            ## chromo is {Task:Node},{Task:Node},... - fixed length
            schedule = builder(chromo, current_time)
            time = Utility.get_the_last_time(schedule)
            print(" fitness end")
            return (1/time,)
        ## TODO: redesign it later
        return fitness

    def build_schedule(self, chromo, fixed_schedule_part, current_time):
        builder = ScheduleBuilder(self.workflow, self.resource_manager, self.estimator, self.task_map, self.node_map, fixed_schedule_part)
        schedule = builder(chromo, current_time)
        return schedule


    def crossover(self, child1, child2):

        ## TODO: only for debug. remove it later.
        ##return

        #estimate size of a chromosome
        size = len([item for (node_name, items) in child1.items() for item in items])
        # return None
        i1 = random.randint(0, size - 1)
        i2 = random.randint(0, size - 1)
        index1 = min(i1, i2)
        index2 = max(i1, i2)

        def chromo_to_seq(chromo):
            result = []
            for (nd_name, items) in chromo.items():
                result += [(nd_name, item) for item in items]
            return result

        def fill_chromo(chromo, seq):
            chromo.clear()
            for node in self.nodes:
                chromo[node.name] = []
            for (nd_name, tsk_id) in seq:
                chromo[nd_name].append(tsk_id)

        ch1 = chromo_to_seq(child1)
        ch2 = chromo_to_seq(child2)

        window = dict()
        for i in range(index1, index2):
            tsk_id = ch1[i][1]
            window[ch1[i][1]] = i

        for i in range(size):
            tsk_id = ch2[i][1]
            if tsk_id in window:
                buf = ch1[window[tsk_id]]
                ch1[window[tsk_id]] = ch2[i]
                ch2[i] = buf

        fill_chromo(child1, ch1)
        fill_chromo(child2, ch2)
        pass

    # def swap_mutation(self, chromo):
    #     node_index = random.randint(0, len(self.nodes) - 1)
    #     node_seq = chromo[self.nodes[node_index].name]
    #
    #     while True:

    def mutation(self, chromosome):
        #return chromosome
        # simply change one node of task mapping
        #TODO: make checking for all nodes are dead.(It's a very rare situation so it is not consider for now)
        alive_nodes = [node for node in self.nodes if node.state != Node.Down]
        node1 = alive_nodes[random.randint(0, len(alive_nodes) - 1)]
        node2 = alive_nodes[random.randint(0, len(alive_nodes) - 1)]

        ch = chromosome[node1.name]
        if len(chromosome[node1.name]) > 0:
            length = len(chromosome[node1.name])
            ind = random.randint(0, length - 1)
            dna = chromosome[node1.name][ind]
            del chromosome[node1.name][ind]
            chromosome[node2.name].append(dna)
        return chromosome

    def sweep_mutation(self, chromosome):

        def is_dependent(tsk1, tsk2):
            for p in tsk1.parents:
                if tsk1.id == tsk2.id:
                    return True
                else:
                    return is_dependent(p, tsk2)
            return False

        #return chromosome
        #TODO: make checking for all nodes are dead.(It's a very rare situation so it is not consider for now)
        alive_nodes = [node for node in self.nodes if node.state != Node.Down]
        node = alive_nodes[random.randint(0, len(alive_nodes) - 1)]


        ch = chromosome[node.name]
        if len(chromosome[node.name]) > 0:
            length = len(chromosome[node.name])
            ind = random.randint(0, length - 1)
            tsk1 = self.task_map[chromosome[node.name][ind]]
            dna = chromosome[node.name][ind]

            count = 0
            while count < 5:
                ind1 = random.randint(0, length - 1)

                tsk2 = self.task_map[chromosome[node.name][ind1]]
                if (not is_dependent(tsk1, tsk2)) and (not is_dependent(tsk2, tsk1)):
                    chromosome[node.name][ind] = chromosome[node.name][ind1]
                    chromosome[node.name][ind1] = dna
                    break
                else:
                    count += 1

        return chromosome

    pass

