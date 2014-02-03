from environment.Resource import ResourceGenerator
from core.DSimpleHeft import DynamicHeft
from core.PublicResourceManager import PublicResourceManager
from core.examples.BaseExecutorExample import BaseExecutorExample
from core.executors.CloudHeftExecutor import CloudHeftExecutor


class CloudHeftExecutorExample(BaseExecutorExample):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None):

        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, with_ga_initial)

        rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4)
                                 ##min_flops=20,
                                ## max_flops=20)

        (public_resources, reliability_map_cloud, probability_estimator) = rgen.generate_public_resources()
        public_resource_manager = PublicResourceManager(public_resources, reliability_map_cloud, probability_estimator)

        dynamic_heft = DynamicHeft(wf, resource_manager, estimator)
        cloud_heft_machine = CloudHeftExecutor(heft_planner=dynamic_heft,
                                               base_fail_duration=40,
                                               base_fail_dispersion=1,
                                               desired_reliability=0.98,
                                               public_resource_manager=public_resource_manager,
                                               #initial_schedule=None)
                                               initial_schedule=initial_schedule)
        cloud_heft_machine.init()
        cloud_heft_machine.run()

        cloud_heft_schedule = cloud_heft_machine.current_schedule
        (makespan, vl1, vl2) = self.extract_result(cloud_heft_schedule, is_silent, wf)
        return makespan

    @staticmethod
    def single_run():
        obj = CloudHeftExecutorExample()
        wf_name = 'CyberShake_75'
        reliability = 0.6
        obj.main(reliability, False, wf_name)



