from datetime import datetime
from GA.DEAPGA.GAImplementation.GAFunctions2 import mark_finished
from GA.DEAPGA.GAImplementation.GAImpl import GAFactory
from core.DSimpleHeft import DynamicHeft
from core.HeftHelper import HeftHelper
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from environment.ResourceGenerator import ResourceGenerator
from environment.ResourceManager import Schedule
from environment.Utility import Utility, profile_decorator
from environment.Utility import GraphVisualizationUtility as viz

DEFAULT_GA_PARAMS = {
    "population": 1000,
    "crossover_probability": 0.8,
    "replacing_mutation_probability": 0.5,
    "sweep_mutation_probability": 0.4,
    "generations": 50
}

class BaseRunner:
    def _construct_environment(self, *args, **kwargs):
        wf_name = kwargs["wf_name"]
        nodes_conf = kwargs.get("nodes_conf", None)
        ideal_flops = kwargs.get("ideal_flops", 20)
        transfer_time = kwargs.get("transfer_time", 100)

        dax1 = '../../resources/' + wf_name + '.xml'
        wf = Utility.readWorkflow(dax1, wf_name)

        rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4,
                                 min_flops=5,
                                 max_flops=10)
        resources = rgen.generate()
        transferMx = rgen.generateTransferMatrix(resources)

        if nodes_conf is None:
            bundle = Utility.get_default_bundle()
            resources = bundle.dedicated_resources
            transferMx = bundle.transfer_mx
            ideal_flops = bundle.ideal_flops
            ##TODO: end
        else:
            ## TODO: refactor it later.
            resources = ResourceGenerator.r(nodes_conf)
            transferMx = rgen.generateTransferMatrix(resources)
            ##

        estimator = ExperimentEstimator(transferMx, ideal_flops, dict(), transfer_time)
        resource_manager = ExperimentResourceManager(resources)
        return (wf, resource_manager, estimator)

    def _validate(self, wf, estimator, schedule):
         max_makespan = Utility.makespan(schedule)
         seq_time_validaty = Utility.validateNodesSeq(schedule)
         mark_finished(schedule)
         dependency_validaty = Utility.validateParentsAndChildren(schedule, wf)
         transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(schedule, wf, estimator)
         print("=============Results====================")
         print("              Makespan %s" % str(max_makespan))
         print("          Seq validaty %s" % str(seq_time_validaty))
         print("   Dependancy validaty %s" % str(dependency_validaty))
         print("    Transfer validaty %s" % str(transfer_dependency_validaty))

    def run(self, *args, **kwargs):
        pass



class MixRunner(BaseRunner):
    def __call__(self, wf_name, ideal_flops, is_silent=False, is_visualized=True, ga_params=DEFAULT_GA_PARAMS, nodes_conf = None, transfer_time=100):

        print("Proccessing " + str(wf_name))

        (wf, resource_manager, estimator) = self._construct_environment(wf_name=wf_name, nodes_conf=nodes_conf, ideal_flops=ideal_flops,transfer_time=transfer_time)

        alg_func = GAFactory.default().create_ga(silent=is_silent,
                                                 wf=wf,
                                                 resource_manager=resource_manager,
                                                 estimator=estimator,
                                                 ga_params=ga_params)

        def _run_heft():
            dynamic_planner = DynamicHeft(wf, resource_manager, estimator)
            nodes = HeftHelper.to_nodes(resource_manager.resources)
            current_cleaned_schedule = Schedule({node: [] for node in nodes})
            schedule_dynamic_heft = dynamic_planner.run(current_cleaned_schedule)

            self._validate(wf, estimator, schedule_dynamic_heft)

            if is_visualized:
                viz.visualize_task_node_mapping(wf, schedule_dynamic_heft)
                # Utility.create_jedule_visualization(schedule_dynamic_heft, wf_name+'_heft')
                pass
            return schedule_dynamic_heft

        # @profile_decorator
        def _run_ga(initial_schedule):
            def default_fixed_schedule_part(resource_manager):
                fix_schedule_part = Schedule({node: [] for node in HeftHelper.to_nodes(resource_manager.get_resources())})
                return fix_schedule_part

            fix_schedule_part = default_fixed_schedule_part(resource_manager)
            ((the_best_individual, pop, schedule, iter_stopped), logbook) = alg_func(fix_schedule_part, initial_schedule)

            self._validate(wf, estimator, schedule)

            name = wf_name +"_bundle"
            path = '../../resources/saved_schedules/' + name + '.json'
            Utility.save_schedule(path, wf_name, resource_manager.get_resources(), estimator.transfer_matrix, ideal_flops, schedule)

            if is_visualized:
                viz.visualize_task_node_mapping(wf, schedule)
                # Utility.create_jedule_visualization(schedule, wf_name+'_ga')
                pass

            return schedule

        def _run_sa(initial_schedule):



            return None

        ##================================
        ##Dynamic Heft Run
        ##================================
        heft_schedule = _run_heft()
        ##===============================================
        ##Simulated Annealing
        ##===============================================
        _run_sa(heft_schedule)
        ##================================
        ##GA Run
        ##================================

         ## TODO: remove time measure
        tstart = datetime.now()
        # ga_schedule = _run_ga(heft_schedule)
        ga_schedule = _run_ga(None)

        tend = datetime.now()
        tres = tend - tstart
        print("Time Result: " + str(tres.total_seconds()))

        #print("Count of nodes: " + str(sum(1 if len(items) > 0 else 0 for n, items in ga_schedule.mapping.items())))

        print("===========================================")
        heft_makespan = Utility.makespan(heft_schedule)
        ga_makespan = Utility.makespan(ga_schedule)
        print("Profit: " + str((1 - ga_makespan/heft_makespan)*100))
        print("===========================================")
        return (ga_makespan, heft_makespan, ga_schedule, heft_schedule)


