from core.examples.BaseExecutorExample import BaseExecutorExample
from core.executors.GAExecutor import GAExecutor


class GAExecutorExample(BaseExecutorExample):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None):
        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)

        ga_machine = GAExecutor(wf,
                            resource_manager,
                            estimator,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            initial_schedule=bundle.ga_schedule)

        ga_machine.init()
        ga_machine.run()

        resulted_schedule = ga_machine.current_schedule
        (makespan, vl1, vl2) = self.extract_result(resulted_schedule, is_silent, wf)
        return makespan

    @staticmethod
    def single_run():
        obj = GAExecutorExample()
        wf_name = 'CyberShake_30'
        reliability = 0.6
        obj.main(reliability, False, wf_name)

