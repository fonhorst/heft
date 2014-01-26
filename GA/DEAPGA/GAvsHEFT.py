import cProfile
import json
import pstats
import random
import io

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
        chrmo_mapping = {task_id: self.node_map[node_name] for (task_id, node_name) in chromo}
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
        index = random.randint(0, self.workflow_size - 1)
        node_index = random.randint(0, len(self.nodes) - 1)
        (task_id, node_name) = chromosome[index]
        mutated = list(chromosome)
        mutated[index] = (task_id, self.nodes[node_index].name)
        return mutated
    pass

def mark_finished(schedule):
    for (node, items) in schedule.mapping.items():
        for item in items:
            item.state = ScheduleItem.FINISHED

def build():
    ##Preparing
    wf_name = 'CyberShake_30'
    ##wf_name = 'Montage_25'
    ##wf_name = 'Epigenomics_24'

    ##wf_name = 'CyberShake_50'
    ##wf_name = 'Montage_50'

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
                                 max_node_count=4)
                                 ##min_flops=20,
                                ## max_flops=20)
    resources = rgen.generate()
    transferMx = rgen.generateTransferMatrix(resources)
    estimator = ExperimentEstimator(transferMx, ideal_flops, dict())
    resource_manager = ExperimentResourceManager(resources)

    nodes = list(HeftHelper.to_nodes(resources))

    def compcost(job, agent):
        return estimator.estimate_runtime(job, agent)

    def commcost(ni, nj, A, B):
        return estimator.estimate_transfer_time(A, B, ni, nj)

    ranking = HeftHelper.build_ranking_func(nodes, compcost, commcost)
    sorted_tasks = ranking(wf)

    ga_functions = GAFunctions(wf, nodes, sorted_tasks, estimator, population)


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
    #toolbox.register("select", tools.selRoulette)

    def main():
        CXPB, MUTPB, NGEN = 0.8, 0.2, 100
        pop = toolbox.population(n=population)
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

        #f = open('..\\..\\resources\\saved_schedules\\' + name + '.json', 'r')
        #deser = json.load(f, object_hook=decoder)
        #f.close()


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

    ##assert(Utility.validateNodesSeq(schedule) is True)
        ##assert(Utility.validateParentsAndChildren(schedule, wf) is True

    ##================================
    ##Dynamic Heft Run
    ##================================
    dynamic_planner = DynamicHeft( wf, resource_manager, estimator)

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
    pr = cProfile.Profile()
    pr.enable()
    main()
    pr.disable()
    s = io.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())




build()
