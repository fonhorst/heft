from environment.Resource import ResourceGenerator
from core.DSimpleHeft import DynamicHeft
from core.PublicResourceManager import PublicResourceManager
from core.examples.ExecutorRunner import ExecutorRunner
from core.executors.CloudHeftExecutor import CloudHeftExecutor


class CloudHeftExecutorRunner(ExecutorRunner):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None):

        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, with_ga_initial)



        cloud_heft_schedule = cloud_heft_machine.current_schedule
        (makespan, vl1, vl2) = self.extract_result(cloud_heft_schedule, is_silent, wf)
        return makespan

    @staticmethod
    def single_run():
        obj = CloudHeftExecutorRunner()
        wf_name = 'CyberShake_75'
        reliability = 0.6
        obj.main(reliability, False, wf_name)



