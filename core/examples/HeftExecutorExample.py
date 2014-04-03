from core.DSimpleHeft import DynamicHeft
from core.examples.ExecutorRunner import ExecutorRunner
from core.executors import EventMachine
from core.executors.HeftExecutor import HeftExecutor


class HeftExecutorRunner(ExecutorRunner):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None, logger=None):

        wf = ExecutorRunner.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = ExecutorRunner.get_infrastructure(bundle, reliability, with_ga_initial)

        ##TODO: look here ! I'm an idiot tasks of wf != tasks of initial_schedule
        dynamic_heft = DynamicHeft(wf, resource_manager, estimator)
        heft_machine = HeftExecutor(heft_planner=dynamic_heft,
                                    base_fail_duration=40,
                                    base_fail_dispersion=1,
                                    #initial_schedule=None)
                                    initial_schedule=initial_schedule,
                                    logger=logger)
        heft_machine.init()
        heft_machine.run()

        ## TODO: remove it later.
        if logger is not None:
            logger.flush()

        (makespan, vl1, vl2) = self.extract_result(heft_machine.current_schedule, is_silent, wf)
        return makespan

    @staticmethod
    def single_run():
        obj = HeftExecutorRunner()
        wf_name = 'CyberShake_75'
        reliability = 0.6
        obj.main(reliability, False, wf_name)