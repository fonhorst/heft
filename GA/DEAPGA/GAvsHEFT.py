import cProfile
from collections import deque
import json
import pstats
import random
import io
from GA.DEAPGA.SimpleRandomizedHeuristic import SimpleRandomizedHeuristic

from deap import base
from deap import creator
from deap import tools
from environment.Utility import Utility, SaveBundle, SaveBundleEncoder
from environment.Resource import ResourceGenerator, Node
from environment.ResourceManager import Schedule, ScheduleItem
from reschedulingheft.DSimpleHeft import DynamicHeft
from reschedulingheft.HeftHelper import HeftHelper
from reschedulingheft.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from reschedulingheft.simple_heft import StaticHeftPlanner


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

## from Buyya
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

            self.initializing_alg = SimpleRandomizedHeuristic(self.workflow, self.nodes, self.estimator)

            self.initial_chromosome = None##GAFunctions.schedule_to_chromosome(initial_schedule)
            pass

    @staticmethod
    def schedule_to_chromosome(schedule):
        def ids(items):
            return [item.job.id for item in items]
        chromosome = {node.name: ids(items) for (node, items) in schedule.mapping.items()}
        return chromosome

    def initial(self):
        return self.random_chromo()

    def random_chromo(self):
        res = random.random()
        # # TODO:
        if res >0.8:
            return self.initial_chromosome
        ##return [self.random_chromo() for j in range(self.size)]
        sched = self.initializing_alg.schedule()
        #TODO: remove it later
        mark_finished(sched)
        seq_time_validaty = Utility.validateNodesSeq(sched)
        dependency_validaty = Utility.validateParentsAndChildren(sched, self.workflow)
        chromo = GAFunctions2.schedule_to_chromosome(sched)
        return chromo

    def build_schedule(self, chromo):
        ready_tasks = [child.id for child in self.workflow.head_task.children]
        finished_tasks = set()

        def is_child_ready(child):
            ids = set([p.id for p in child.parents])
            result = False in [id in finished_tasks for id in ids]
            return not result

        schedule_mapping = {node: [] for node in self.nodes}

        chrmo_mapping = dict()
        for (node_name, tasks) in chromo.items():
            for tsk_id in tasks:
                chrmo_mapping[tsk_id] = self.node_map[node_name]
        #chrmo_mapping = {task_id: self.node_map[node_name] for (task_id, node_name) in chromo.items()}
        task_to_node = dict()
        estimate = self.estimator.estimate_transfer_time

        def find_slots(node, comm_ready, runtime):
            node_schedule = schedule_mapping.get(node, list())
            free_time = 0 if len(node_schedule) == 0 else node_schedule[-1].end_time
            ## TODO: refactor it later
            f_time = max(free_time, comm_ready)
            base_variant = [(f_time, f_time + runtime)]
            zero_interval = [] if len(node_schedule) == 0 else [(0, node_schedule[0].start_time)]
            middle_intervals = [(node_schedule[i].end_time, node_schedule[i + 1].start_time) for i in range(len(node_schedule) - 1)]
            intervals = zero_interval + middle_intervals + base_variant

            result = [(st, end) for (st, end) in intervals if st >= comm_ready and abs((end - st) - runtime) < 0.01]
            return result

        def comm_ready_func(task, node):
            ##TODO: remake this stub later.
            if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
                return 0
            return max([task_to_node[p.id][2]+estimate(node, chrmo_mapping[p.id], task, p) for p in task.parents])

        def get_possible_execution_times(task, node):
            ## pay attention to the last element in the resulted seq
            ## it represents all available time of node after it completes all its work
            ## (if such interval can exist)
            ## time_slots = [(st1, end1),(st2, end2,...,(st_last, st_last + runtime)]
            runtime = self.estimator.estimate_runtime(task, node)
            comm_ready = comm_ready_func(task, node)
            time_slots = find_slots(node, comm_ready, runtime)
            return time_slots, runtime

        chromo_copy = dict()
        for (nd_name, items) in chromo.items():
            chromo_copy[nd_name] = []
            for item in items:
                chromo_copy[nd_name].append(item)


        while len(ready_tasks) > 0:
            for node in self.nodes:
                if len(chromo_copy[node.name]) == 0:
                    continue
                #tsk_id = chromo_copy[node.name][0]
                tsk_id = None
                for i in range(len(chromo_copy[node.name])):
                    if chromo_copy[node.name][i] in ready_tasks:
                        tsk_id = chromo_copy[node.name][i]

                if tsk_id is not None:
                    task = self.task_map[tsk_id]
                    #del chromo_copy[node.name][0]
                    chromo_copy[node.name].remove(tsk_id)
                    ready_tasks.remove(tsk_id)

                    time_slots, runtime = get_possible_execution_times(task, node)

                    time_slot = time_slots[0]
                    start_time = time_slot[0]
                    end_time = start_time + runtime

                    item = ScheduleItem(task, start_time, end_time)
                    ##schedule_mapping[node].append(item)
                    Schedule.insert_item(schedule_mapping, node, item)
                    task_to_node[task.id] = (node, start_time, end_time)

                    finished_tasks.add(task.id)

                    ready_children = [child for child in task.children if is_child_ready(child)]
                    for child in ready_children:
                        ready_tasks.append(child.id)

        #while len(ready_tasks) > 0:
        #    for node in self.nodes:
        #
        #        while True:
        #            if len(chromo_copy[node.name]) == 0:
        #                break
        #            #tsk_id = chromo_copy[node.name][0]
        #            tsk_id = None
        #            for i in range(len(chromo_copy[node.name])):
        #                if chromo_copy[node.name][i] in ready_tasks:
        #                    tsk_id = chromo_copy[node.name][i]
        #
        #            if tsk_id is not None:
        #                task = self.task_map[tsk_id]
        #                #del chromo_copy[node.name][0]
        #                chromo_copy[node.name].remove(tsk_id)
        #                ready_tasks.remove(tsk_id)
        #
        #                time_slots, runtime = get_possible_execution_times(task, node)
        #
        #                time_slot = time_slots[0]
        #                start_time = time_slot[0]
        #                end_time = start_time + runtime
        #
        #                item = ScheduleItem(task, start_time, end_time)
        #                ##schedule_mapping[node].append(item)
        #                Schedule.insert_item(schedule_mapping, node, item)
        #                task_to_node[task.id] = (node, start_time, end_time)
        #
        #                finished_tasks.add(task.id)
        #
        #                ready_children = [child for child in task.children if is_child_ready(child)]
        #                for child in ready_children:
        #                    ready_tasks.append(child.id)
        #            else:
        #                break

        schedule = Schedule(schedule_mapping)
        return schedule

    def fitness(self, chromo):
        ## value of fitness function is the last time point in the schedule
        ## built from the chromo
        ## chromo is {Task:Node},{Task:Node},... - fixed length
        schedule = self.build_schedule(chromo)
        time = Utility.get_the_last_time(schedule)
        return (1/time,)

    def crossover(self, child1, child2):
        #return None
        i1 = random.randint(0, self.workflow_size - 1)
        i2 = random.randint(0, self.workflow_size - 1)
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

        for i in range(self.workflow_size):
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
        node1 = self.nodes[random.randint(0, len(self.nodes) - 1)]
        node2 = self.nodes[random.randint(0, len(self.nodes) - 1)]

        ch = chromosome[node1.name]
        if len(chromosome[node1.name]) > 0:
            length = len(chromosome[node1.name])
            ind = random.randint(0,length - 1)
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
        node = self.nodes[random.randint(0, len(self.nodes) - 1)]
        ch = chromosome[node.name]
        if len(chromosome[node.name]) > 0:
            length = len(chromosome[node.name])
            ind = random.randint(0,length - 1)
            tsk1 = self.task_map[chromosome[node.name][ind]]
            dna = chromosome[node.name][ind]

            count = 0
            while count < 5:
                ind1 = random.randint(0,length - 1)

                tsk2 = self.task_map[chromosome[node.name][ind1]]
                if (not is_dependent(tsk1, tsk2)) and (not is_dependent(tsk2, tsk1)):
                    chromosome[node.name][ind] = chromosome[node.name][ind1]
                    chromosome[node.name][ind1] = dna
                    break
                else:
                    count += 1

        return chromosome




    pass

def mark_finished(schedule):
    for (node, items) in schedule.mapping.items():
        for item in items:
            item.state = ScheduleItem.FINISHED

def build(wf_name , is_silent=False):
    print("Proccessing " + str(wf_name))
    ##Preparing
    #wf_name = 'CyberShake_30'
    # wf_name = 'CyberShake_50'
    # wf_name = 'CyberShake_100'

    # wf_name = 'Montage_25'
    # wf_name = 'Montage_50'
    # wf_name = 'Montage_100'

    # wf_name = 'Epigenomics_24'
    # wf_name = 'Epigenomics_46'
    # wf_name = 'Epigenomics_100'

    # wf_name = "Inspiral_30"
    # wf_name = "Inspiral_50"
    # wf_name = "Inspiral_100"

    # wf_name = "Sipht_30"
    # wf_name = "Sipht_60"
    # wf_name = "Sipht_100"


    dax1 = '..\\..\\resources\\' + wf_name + '.xml'
    ##dax1 = '..\\..\\resources\\Montage_50.xml'

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000
    ideal_flops = 20
    population = 300

    wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
    rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4,
                                 min_flops=5,
                                 max_flops=10)
    resources = rgen.generate()
    transferMx = rgen.generateTransferMatrix(resources)

    ##TODO: remove it later
    dax2 = '..\\..\\resources\\' + 'CyberShake_30' + '.xml'
    path = '..\\..\\resources\\saved_schedules\\' + 'CyberShake_30_bundle_backup' + '.json'
    bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1))
    resources = bundle.dedicated_resources
    transferMx = bundle.transfer_mx
    ideal_flops = bundle.ideal_flops
    ##TODO: end

    estimator = ExperimentEstimator(transferMx, ideal_flops, dict())
    resource_manager = ExperimentResourceManager(resources)

    nodes = list(HeftHelper.to_nodes(resources))

    def compcost(job, agent):
        return estimator.estimate_runtime(job, agent)

    def commcost(ni, nj, A, B):
        return estimator.estimate_transfer_time(A, B, ni, nj)

    ranking = HeftHelper.build_ranking_func(nodes, compcost, commcost)
    sorted_tasks = ranking(wf)

    #ga_functions = GAFunctions(wf, nodes, sorted_tasks, estimator, population)
    ga_functions = GAFunctions2(wf, nodes, sorted_tasks, estimator, population)


    ##================================
    ##Create genetic algorithm here
    ##================================
    creator.create("FitnessMax", base.Fitness, weights=(1.0,))
    creator.create("Individual", dict, fitness=creator.FitnessMax)

    toolbox = base.Toolbox()
    # Attribute generator
    toolbox.register("attr_bool", ga_functions.initial)
    # Structure initializers
    toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.attr_bool)
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)



    toolbox.register("evaluate", ga_functions.fitness)
    # toolbox.register("mate", tools.cxOnePoint)
    # toolbox.register("mate", tools.cxTwoPoints)
    # toolbox.register("mate", tools.cxUniform, indpb=0.2)

    toolbox.register("mate", ga_functions.crossover)
    toolbox.register("mutate", ga_functions.mutation)
    toolbox.register("select", tools.selTournament, tournsize=10)
    #toolbox.register("select", tools.selRoulette)

    def main(initial_schedule):
        #ga_functions.initial_chromosome = GAFunctions.schedule_to_chromosome(initial_schedule, sorted_tasks)
        ga_functions.initial_chromosome = GAFunctions2.schedule_to_chromosome(initial_schedule)
        CXPB, MUTPB, NGEN = 0.8, 0.5, 50
        SWEEPMUTPB = 0.0
        pop = toolbox.population(n=population)
        # Evaluate the entire population
        fitnesses = list(map(toolbox.evaluate, pop))
        for ind, fit in zip(pop, fitnesses):
            ind.fitness.values = fit
        # Begin the evolution
        print("Evaluating...")
        for g in range(NGEN):
            # print("-- Generation %i --" % g)
            # Select the next generation individuals
            offspring = toolbox.select(pop, len(pop))
            # Clone the selected individuals
            offspring = list(map(toolbox.clone, offspring))
            # Apply crossover and mutation on the offspring
            for child1, child2 in zip(offspring[::2], offspring[1::2]):
                if random.random() < CXPB:
                    toolbox.mate(child1, child2)
                    del child1.fitness.values
                    del child2.fitness.values

            for mutant in offspring:
                if random.random() < SWEEPMUTPB:
                    ga_functions.sweep_mutation(mutant)
                    del mutant.fitness.values
                if random.random() < MUTPB:
                    toolbox.mutate(mutant)
                    del mutant.fitness.values
            # Evaluate the individuals with an invalid fitness
            invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
            fitnesses = map(toolbox.evaluate, invalid_ind)
            for ind, fit in zip(invalid_ind, fitnesses):
                ind.fitness.values = fit
            pop[:] = offspring
            # Gather all the fitnesses in one list and print the stats
            fits = [ind.fitness.values[0] for ind in pop]

            length = len(pop)
            mean = sum(fits) / length
            sum2 = sum(x*x for x in fits)
            std = abs(sum2 / length - mean**2)**0.5

            if not is_silent:
                print("-- Generation %i --" % g)
                print("  Worst %s" % str(1/min(fits)))
                print("   Best %s" % str(1/max(fits)))
                print("    Avg %s" % str(1/mean))
                print("    Std %s" % str(1/std))
        pass

        ##TODO: remove it later
       ##print("======END-END======")
        ##return None

        resulted_pop = [(ind, ind.fitness.values[0]) for ind in pop]
        result = max(resulted_pop, key=lambda x: x[1])
        schedule = ga_functions.build_schedule(result[0])
        seq_time_validaty = Utility.validateNodesSeq(schedule)
        mark_finished(schedule)
        dependency_validaty = Utility.validateParentsAndChildren(schedule, wf)
        print("=============Results====================")
        print("              Makespan %s" % str(1/result[1]))
        print("          Seq validaty %s" % str(seq_time_validaty))
        print("   Dependancy validaty %s" % str(dependency_validaty))

        name = wf_name +"_bundle"
        path = '..\\..\\resources\\saved_schedules\\' + name + '.json'
        Utility.save_schedule(path, wf_name, resources, transferMx, ideal_flops, schedule)

        res = Utility.load_schedule(path, wf)
        res1 = Utility.load_schedule(path, wf)
        pass




    ##================================
    ##Heft Run
    ##================================
    planner = StaticHeftPlanner()
    planner.estimator = estimator
    planner.resource_manager = resource_manager
    planner.workflows = [wf]

    schedule_heft = planner.schedule()
    heft_makespan = Utility.get_the_last_time(schedule_heft)
    seq_time_validaty = Utility.validateNodesSeq(schedule_heft)
    mark_finished(schedule_heft)
    dependency_validaty = Utility.validateParentsAndChildren(schedule_heft, wf)
    print("heft_makespan: " + str(heft_makespan))
    print("=============HEFT Results====================")
    print("              Makespan %s" % str(heft_makespan))
    print("          Seq validaty %s" % str(seq_time_validaty))
    print("   Dependancy validaty %s" % str(dependency_validaty))

    ##chromo = GAFunctions.schedule_to_chromosome(schedule_heft, sorted_tasks)
    ##sched = ga_functions.build_schedule(chromo)
    k = 0
    ##assert(Utility.validateNodesSeq(schedule) is True)
        ##assert(Utility.validateParentsAndChildren(schedule, wf) is True

    ##================================
    ##Dynamic Heft Run
    ##================================
    dynamic_planner = DynamicHeft(wf, resource_manager, estimator)

    nodes = HeftHelper.to_nodes(resource_manager.resources)
    current_cleaned_schedule = Schedule({node: [] for node in nodes})
    schedule_dynamic_heft = dynamic_planner.run(current_cleaned_schedule)
    dynamic_heft_makespan = Utility.get_the_last_time(schedule_dynamic_heft)
    dynamic_seq_time_validaty = Utility.validateNodesSeq(schedule_dynamic_heft)
    mark_finished(schedule_dynamic_heft)
    dynamic_dependency_validaty = Utility.validateParentsAndChildren(schedule_dynamic_heft, wf)
    print("heft_makespan: " + str(heft_makespan))
    print("=============Dynamic HEFT Results====================")
    print("              Makespan %s" % str(dynamic_heft_makespan))
    print("          Seq validaty %s" % str(dynamic_seq_time_validaty))
    print("   Dependancy validaty %s" % str(dynamic_dependency_validaty))


    ##================================
    ##GA Run
    ##================================
    #pr = cProfile.Profile()
    #pr.enable()
    main(schedule_heft)
    #pr.disable()
    #s = io.StringIO()
    #sortby = 'cumulative'
    #ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    #ps.print_stats()
    #print(s.getvalue())



wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_100",
            "Montage_25", "Montage_50", "Montage_100",
            "Epigenomics_24", "Epigenomics_46", "Epigenomics_100",
            "Inspiral_30", "Inspiral_50", "Inspiral_100",
            "Sipht_30", "Sipht_60", "Sipht_100"]
[build(wf_name, True) for wf_name in wf_names]
