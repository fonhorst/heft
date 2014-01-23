import random

from deap import base
from deap import creator
from deap import tools
from environment.Utility import Utility
from environment.Resource import ResourceGenerator
from environment.ResourceManager import Schedule, ScheduleItem
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
            pass

    def initial(self):
        ##return [self.random_chromo() for j in range(self.size)]
        return self.random_chromo()

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
        chrmo_mapping = {task.id: node for (task, node) in chromo}
        task_to_node = dict()
        estimate = self.estimator.estimate_transfer_time

        def max_parent_finish(task):
            ##TODO: remake this stub later
            ##TODO: (self.workflow.head_task == list(task.parents)[0]) - False. It's a bug.
            ## fix it later.
            if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
                return 0
            return max([task_to_node[p.id][2] for p in task.parents])

        def transfer(task, node):
            ## find all parent nodes mapping
            ## estimate with estimator transfer time for it
            ##TODO: remake this stub later.
            if len(task.parents) == 1 and self.workflow.head_task.id == list(task.parents)[0].id:
                return 0
            lst = [estimate(node, chrmo_mapping[parent.id], task, parent) for parent in task.parents]
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
            task_to_node[task.id] = (node, start_time, end_time)

        schedule = Schedule(schedule_mapping)
        return schedule

    def random_chromo(self):
        nodes_len = len(self.nodes)
        def choose_node():
            index = random.randint(0, nodes_len - 1)
            return self.nodes[index]
        return [(self.sorted_tasks[i], choose_node()) for i in range(self.workflow_size)]

    def mutation(self, chromosome):
        # simply change one node of task mapping
        index = random.randint(0, self.workflow_size - 1)
        node_index = random.randint(0, len(self.nodes) - 1)
        (task, node) = chromosome[index]
        mutated = list(chromosome)
        mutated[index] = (task, self.nodes[node_index])
        return mutated
    pass

def build():
    ##Preparing
    ##dax1 = '..\\..\\resources\\CyberShake_30.xml'
    dax1 = '..\\..\\resources\\Montage_50.xml'

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000
    ideal_flops = 2

    wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
    rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4)
                                 ##min_flops=20,
                                ## max_flops=20)
    resources = rgen.generate()
    transferMx = rgen.generateTransferMatrix(resources)
    estimator = ExperimentEstimator(transferMx, ideal_flops)

    nodes = list(HeftHelper.to_nodes(resources))

    def compcost(job, agent):
        return estimator.estimate_runtime(job, agent)

    def commcost(ni, nj, A, B):
        return estimator.estimate_transfer_time(A, B, ni, nj)

    ranking = HeftHelper.build_ranking_func(nodes, compcost, commcost)
    sorted_tasks = ranking(wf)

    ga_functions = GAFunctions(wf, nodes, sorted_tasks, estimator, 300)


    ##================================
    ##Create genetic algorithm here
    ##================================
    creator.create("FitnessMax", base.Fitness, weights=(1.0,))
    creator.create("Individual", list, fitness=creator.FitnessMax)

    toolbox = base.Toolbox()
    # Attribute generator
    toolbox.register("attr_bool", ga_functions.initial)
    # Structure initializers
    toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.attr_bool)
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)



    toolbox.register("evaluate", ga_functions.fitness)
    toolbox.register("mate", tools.cxTwoPoints)
    toolbox.register("mutate", ga_functions.mutation)
    toolbox.register("select", tools.selTournament, tournsize=3)

    def main():
        CXPB, MUTPB, NGEN = 0.5, 0.2, 40
        pop = toolbox.population(n=300)
        # Evaluate the entire population
        fitnesses = list(map(toolbox.evaluate, pop))
        for ind, fit in zip(pop, fitnesses):
            ind.fitness.values = fit
        # Begin the evolution
        for g in range(NGEN):
            print("-- Generation %i --" % g)
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

            print("  Worst %s" % str(1/min(fits)))
            print("   Best %s" % str(1/max(fits)))
            print("    Avg %s" % str(1/mean))
            print("    Std %s" % str(1/std))
        pass

        resulted_pop = [(ind, ind.fitness.values[0]) for ind in pop]
        result = max(resulted_pop, lambda x: x[1])
        schedule = ga_functions.build_schedule(result[0])
        seq_time_validaty = Utility.validateNodesSeq(schedule)
        dependency_validaty = Utility.validateParentsAndChildren(schedule, wf)
        print("=============Results====================")
        print("              Makespan %s" % str(1/result[1]))
        print("          Seq validaty %s" % str(seq_time_validaty))
        print("   Dependancy validaty %s" % str(dependency_validaty))

    ##================================
    ##Heft Run
    ##================================
    planner = StaticHeftPlanner()
    planner.estimator = estimator
    planner.resource_manager = ExperimentResourceManager(resources)
    planner.workflows = [wf]

    schedule_heft = planner.schedule()
    heft_makespan = Utility.get_the_last_time(schedule_heft)
    print("heft_makespan: " + str(heft_makespan))

    ##================================
    ##GA Run
    ##================================
    main()



build()
