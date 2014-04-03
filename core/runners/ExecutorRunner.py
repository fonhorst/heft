from core.DSimpleHeft import DynamicHeft
from core.PublicResourceManager import PublicResourceManager
from core.comparisons.ComparisonBase import ResultSaver
from core.comparisons.StopRescheduling import GaOldPopExecutor
from core.executors.CloudHeftExecutor import CloudHeftExecutor
from core.executors.GAExecutor import GAExecutor
from core.executors.GaHeftExecutor import GaHeftExecutor
from core.executors.HeftExecutor import HeftExecutor
from core.tests.FailExperiment import SingleFailGaHeftExecutor, SingleFailHeftExecutor
from environment.Resource import ResourceGenerator
from environment.Utility import Utility
from core.HeftHelper import HeftHelper
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager



class ExecutorRunner:
    def __init__(self):
        pass

    @staticmethod
    def get_wf(wf_name, task_postfix_id="00"):
        ## TODO: make check for valid path in wf_name variable
        dax1 = '..\\..\\resources\\' + wf_name + '.xml'
        wf = Utility.readWorkflow(dax1, task_postfix_id=task_postfix_id)
        return wf

    def get_bundle(self, the_bundle):
        bundle = None
        if the_bundle is None:
            bundle = Utility.get_default_bundle()
        else:
            bundle = the_bundle
        return bundle

    @staticmethod
    def get_infrastructure(bundle, reliability, with_ga_initial):

        nodes = HeftHelper.to_nodes(bundle.dedicated_resources)
        realibility_map = {node.name: reliability for node in nodes}

        initial_schedule = None
        if with_ga_initial is True:
            initial_schedule = bundle.ga_schedule
            #initial_ga_makespan = Utility.get_the_last_time(initial_schedule )
            #print("Initial GA makespan: " + str(initial_ga_makespan))
            ## TODO: end

        ##======================
        ## create heft_executor
        ##======================
        estimator = ExperimentEstimator(bundle.transfer_mx, bundle.ideal_flops, realibility_map)
        resource_manager = ExperimentResourceManager(bundle.dedicated_resources)
        return (estimator, resource_manager, initial_schedule)

    def main(self, reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None):
        pass

    def extract_result(self, schedule, is_silent, wf):
        makespan = Utility.get_the_last_time(schedule)
        seq_time_validaty = Utility.validateNodesSeq(schedule)
        dependency_validaty = Utility.validateParentsAndChildren(schedule, wf)
        if not is_silent:
            print("=============Res Results====================")
            print("              Makespan %s" % str(makespan))
            print("          Seq validaty %s" % str(seq_time_validaty))
            print("   Dependancy validaty %s" % str(dependency_validaty))
        return (makespan, seq_time_validaty, dependency_validaty)

    def _get_executor(self):
        pass

    def __call__(self, func):
        #reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None, logger=None
        def wrapper(*args, **kwargs):

            wf_name = kwargs[       'wf_name'       ]
            reliability = kwargs[   'reliability'   ]
            is_silent = kwargs[     'is_silent'     ]

            the_bundle = kwargs.get(        'the_bundle', None)
            with_ga_initial = kwargs.get(   'with_ga_initial', None)
            logger = kwargs.get(            'logger', None)


            wf = ExecutorRunner.get_wf(wf_name)
            bundle = self.get_bundle(the_bundle)
            (estimator, resource_manager, initial_schedule) = ExecutorRunner.get_infrastructure(bundle, reliability, with_ga_initial)

            kwargs["wf"] = wf
            kwargs["estimator"] = estimator
            kwargs["resource_manager"] = resource_manager
            kwargs["initial_schedule"] = initial_schedule

            result = func(*args, **kwargs)

            if logger is not None:
                logger.flush()

            (makespan, vl1, vl2) = self.extract_result(result, is_silent, wf)
            return makespan

        return wrapper
    pass


class ExecutorsFactory:

    DEFAULT_SAVE_PATH = "../../resources/saved_simulation_results"

    _default = None

    @staticmethod
    def default():
        if ExecutorsFactory._default is None:
            ExecutorsFactory._default = ExecutorsFactory()
        return ExecutorsFactory._default

    @ExecutorRunner
    def run_ga_executor(self, *args, **kwargs):
        ga_machine = GAExecutor(kwargs["wf"],
                            kwargs["resource_manager"],
                            kwargs["estimator"],
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            initial_schedule=kwargs["init_schedule"])

        ga_machine.init()
        ga_machine.run()
        resulted_schedule = ga_machine.current_schedule
        return resulted_schedule

    @ExecutorRunner
    def run_heft_executor(self, *args, **kwargs):
        ##TODO: look here ! I'm an idiot tasks of wf != tasks of initial_schedule
        dynamic_heft = DynamicHeft(kwargs["wf"], kwargs["resource_manager"], kwargs["estimator"])
        heft_machine = HeftExecutor(heft_planner=dynamic_heft,
                                    base_fail_duration=40,
                                    base_fail_dispersion=1,
                                    #initial_schedule=None)
                                    initial_schedule=kwargs["initial_schedule"],
                                    logger=kwargs["logger"])
        heft_machine.init()
        heft_machine.run()
        resulted_schedule = heft_machine.current_schedule
        return resulted_schedule

    @ExecutorRunner
    def run_gaheft_executor(self, *args, **kwargs):
        dynamic_heft = DynamicHeft(kwargs["wf"], kwargs["resource_manager"], kwargs["estimator"])
        ga_heft_machine = GaHeftExecutor(
                            heft_planner=dynamic_heft,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            fixed_interval_for_ga=kwargs["fixed_interval_for_ga"],
                            logger=kwargs["logger"])

        ga_heft_machine.init()
        ga_heft_machine.run()

        resulted_schedule = ga_heft_machine.current_schedule
        return resulted_schedule

    @ExecutorRunner
    def run_cloudheft_executor(self, *args, **kwargs):
        rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4)
                                 ##min_flops=20,
                                ## max_flops=20)

        (public_resources, reliability_map_cloud, probability_estimator) = rgen.generate_public_resources()
        public_resource_manager = PublicResourceManager(public_resources, reliability_map_cloud, probability_estimator)

        dynamic_heft = DynamicHeft(kwargs["wf"], kwargs["resource_manager"], kwargs["estimator"])
        cloud_heft_machine = CloudHeftExecutor(heft_planner=dynamic_heft,
                                               base_fail_duration=40,
                                               base_fail_dispersion=1,
                                               desired_reliability=0.98,
                                               public_resource_manager=public_resource_manager,
                                               #initial_schedule=None)
                                               initial_schedule=kwargs["initial_schedule"])
        cloud_heft_machine.init()
        cloud_heft_machine.run()

        resulted_schedule = cloud_heft_machine.current_schedule
        return resulted_schedule

    @ExecutorRunner
    def run_oldpop_executor(self, *args, **kwargs):
        stat_saver = ResultSaver(self.DEFAULT_SAVE_PATH.format(kwargs["key_for_save"]))

        ga_machine = GaOldPopExecutor(
                            workflow=kwargs["wf"],
                            resource_manager=kwargs["resource_manager"],
                            estimator=kwargs["estimator"],
                            ga_params=kwargs["ga_params"],
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            wf_name=kwargs["wf_name"],
                            stat_saver=stat_saver,
                            task_id_to_fail=kwargs["task_id_to_fail"],
                            logger=kwargs["logger"])

        ga_machine.init()
        ga_machine.run()

        resulted_schedule = ga_machine.current_schedule
        return resulted_schedule

    @ExecutorRunner
    def run_singlefail_heft_executor(self, *args, **kwargs):
        ##TODO: look here ! I'm an idiot tasks of wf != tasks of initial_schedule
        dynamic_heft = DynamicHeft(kwargs["wf"], kwargs["resource_manager"], kwargs["estimator"])
        heft_machine = SingleFailHeftExecutor(heft_planner=dynamic_heft,
                                    base_fail_duration=40,
                                    base_fail_dispersion=1,
                                    #initial_schedule=None)
                                    initial_schedule=kwargs["initial_schedule"],
                                    logger=kwargs["logger"],
                                    task_id_to_fail=kwargs["task_id_to_fail"],
                                    failure_coeff=kwargs["failure_coeff"])
        heft_machine.init()
        heft_machine.run()

        resulted_schedule = heft_machine.current_schedule
        return resulted_schedule

    @ExecutorRunner
    def run_singlefail_gaheft_executor(self, *args, **kwargs):
        dynamic_heft = DynamicHeft(kwargs["wf"], kwargs["resource_manager"], kwargs["estimator"])
        ga_heft_machine = SingleFailGaHeftExecutor(
                            heft_planner=dynamic_heft,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            fixed_interval_for_ga=kwargs["fixed_interval_for_ga"],
                            logger=kwargs["logger"],
                            task_id_to_fail=kwargs["task_id_to_fail"])

        ga_heft_machine.init()
        ga_heft_machine.run()

        resulted_schedule = ga_heft_machine.current_schedule
        return resulted_schedule

    pass
