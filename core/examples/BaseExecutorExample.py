from environment.Utility import Utility
from core.HeftHelper import HeftHelper
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager



class BaseExecutorExample:
    def __init__(self):
        pass

    @staticmethod
    def get_wf(wf_name, task_postfix_id="00"):
        ## TODO: make check for valid path in wf_name variable
        dax1 = '..\\..\\resources\\' + wf_name + '.xml'
        wf = Utility.readWorkflow(dax1, task_postfix_id=task_postfix_id)
        return wf

    @staticmethod
    def get_default_bundle():
        ## dedicated resource are the same for all bundles
        dax2 = '..\\..\\resources\\' + 'CyberShake_30' + '.xml'
        path = '..\\..\\resources\\saved_schedules\\' + 'CyberShake_30_bundle_backup' + '.json'
        bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2))
        return bundle

    def get_bundle(self, the_bundle):
        bundle = None
        if the_bundle is None:
            bundle = BaseExecutorExample.get_default_bundle()
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
