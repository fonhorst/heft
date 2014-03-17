from GA.DEAPGA.GAImplementation.GAFunctions2 import mark_finished
from GA.DEAPGA.GAImplementation.GAImpl import GAFactory
from core.DSimpleHeft import DynamicHeft
from core.HeftHelper import HeftHelper
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from environment.Resource import ResourceGenerator
from environment.ResourceManager import Schedule
from environment.Utility import Utility, profile_decorator
from environment.Utility import GraphVisualizationUtility as viz

DEFAULT_GA_PARAMS = {
    "population": 50,
    "crossover_probability": 0.8,
    "replacing_mutation_probability": 0.5,
    "sweep_mutation_probability": 0.4,
    "generations": 100
}

def run(wf_name, ideal_flops, is_silent=False, is_visualized=True, ga_params=DEFAULT_GA_PARAMS, nodes_conf = None):
    print("Proccessing " + str(wf_name))

    dax1 = '../../resources/' + wf_name + '.xml'

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000
    ideal_flops = ideal_flops

    wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
    rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4,
                                 min_flops=5,
                                 max_flops=10)
    resources = rgen.generate()
    transferMx = rgen.generateTransferMatrix(resources)

    if nodes_conf is None:
        ##TODO: remove it later
        dax2 = '../../resources/' + 'CyberShake_30' + '.xml'
        path = '../../resources/saved_schedules/' + 'CyberShake_30_bundle_backup' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1))
        resources = bundle.dedicated_resources
        #transferMx = bundle.transfer_mx
        ideal_flops = bundle.ideal_flops
        ##TODO: end
    else:
        ## TODO: refactor it later.
        resources = ResourceGenerator.r(nodes_conf)
        ##


    estimator = ExperimentEstimator(transferMx, ideal_flops, dict())
    resource_manager = ExperimentResourceManager(resources)
    alg_func = GAFactory.default().create_ga(silent=is_silent,
                                             wf=wf,
                                             resource_manager=resource_manager,
                                             estimator=estimator,
                                             ga_params=ga_params)
    # @profile_decorator
    def run_ga(initial_schedule):
        def default_fixed_schedule_part(resource_manager):
            fix_schedule_part = Schedule({node: [] for node in HeftHelper.to_nodes(resource_manager.get_resources())})
            return fix_schedule_part
        fix_schedule_part = default_fixed_schedule_part(resource_manager)
        (the_best_individual, pop, schedule, iter_stopped) = alg_func(fix_schedule_part, initial_schedule)

        max_makespan = Utility.get_the_last_time(schedule)
        seq_time_validaty = Utility.validateNodesSeq(schedule)
        mark_finished(schedule)
        dependency_validaty = Utility.validateParentsAndChildren(schedule, wf)
        transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(schedule_dynamic_heft, wf, estimator)
        print("=============Results====================")
        print("              Makespan %s" % str(max_makespan))
        print("          Seq validaty %s" % str(seq_time_validaty))
        print("   Dependancy validaty %s" % str(dependency_validaty))
        print("    Transfer validaty %s" % str(transfer_dependency_validaty))

        name = wf_name +"_bundle"
        path = '../../resources/saved_schedules/' + name + '.json'
        Utility.save_schedule(path, wf_name, resources, transferMx, ideal_flops, schedule)



        if is_visualized:
            viz.visualize_task_node_mapping(wf, schedule)
            # Utility.create_jedule_visualization(schedule, wf_name+'_ga')
        pass

        return schedule


    ##================================
    ##Dynamic Heft Run
    ##================================
    dynamic_planner = DynamicHeft(wf, resource_manager, estimator)

    nodes = HeftHelper.to_nodes(resource_manager.resources)
    current_cleaned_schedule = Schedule({node: [] for node in nodes})
    schedule_dynamic_heft = dynamic_planner.run(current_cleaned_schedule)
    dynamic_heft_makespan = Utility.get_the_last_time(schedule_dynamic_heft)
    # validation
    dynamic_seq_time_validaty = Utility.validateNodesSeq(schedule_dynamic_heft)
    mark_finished(schedule_dynamic_heft)
    dynamic_dependency_validaty = Utility.validateParentsAndChildren(schedule_dynamic_heft, wf)
    transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(schedule_dynamic_heft, wf, estimator)
    #printing
    print("heft_makespan: " + str(dynamic_heft_makespan))
    print("=============Dynamic HEFT Results====================")
    print("              Makespan %s" % str(dynamic_heft_makespan))
    print("          Seq validaty %s" % str(dynamic_seq_time_validaty))
    print("   Dependancy validaty %s" % str(dynamic_dependency_validaty))
    print("    Transfer validaty %s" % str(transfer_dependency_validaty))

    if is_visualized:
        viz.visualize_task_node_mapping(wf, schedule_dynamic_heft)
        # Utility.create_jedule_visualization(schedule_dynamic_heft, wf_name+'_heft')


    ##================================
    ##GA Run
    ##================================

    ga_schedule = run_ga(schedule_dynamic_heft)

    print("===========================================")
    heft_makespan = Utility.get_the_last_time(schedule_dynamic_heft)
    ga_makespan = Utility.get_the_last_time(ga_schedule)
    print("Profit: " + str((1 - ga_makespan/heft_makespan)*100))
    print("===========================================")
    pass


