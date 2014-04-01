from core.DSimpleHeft import DynamicHeft
from core.examples.BaseExecutorExample import BaseExecutorExample
from core.executors.GaHeftExecutor import GaHeftExecutor


class GaHeftExecutorExample(BaseExecutorExample):
    def __init__(self, fixed_interval_for_ga=15, logger=None):
        self.fixed_interval_for_ga = fixed_interval_for_ga
        pass

    def main(self, reliability, is_silent, wf_name, the_bundle=None, logger=None):
        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)

        dynamic_heft = DynamicHeft(wf, resource_manager, estimator)
        ga_heft_machine = GaHeftExecutor(
                            heft_planner=dynamic_heft,
                            base_fail_duration=120,
                            base_fail_dispersion=1,
                            fixed_interval_for_ga=self.fixed_interval_for_ga,
                            logger=logger)

        ga_heft_machine.init()
        ga_heft_machine.run()

        resulted_schedule = ga_heft_machine.current_schedule
        (makespan, vl1, vl2) = self.extract_result(resulted_schedule, is_silent, wf)
        return makespan

    @staticmethod
    def single_run():
        obj = GaHeftExecutorExample()
        wf_name = 'Montage_25'
        reliability = 0.95
        obj.main(reliability, False, wf_name)

