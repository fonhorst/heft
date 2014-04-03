from GA.DEAPGA.GAImplementation.GAImpl import GAFactory
from core.examples.ExecutorRunner import ExecutorRunner
from core.executors.GAExecutor import GAExecutor
from environment.ResourceManager import Schedule


class GAExecutorRunner(ExecutorRunner):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, with_ga_initial=False, the_bundle=None, logger=None, use_saved_schedule=True):
        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(the_bundle)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)


        # if use_saved_schedule:
        #     params = {
        #         "population": 50,
        #         "crossover_probability": 0.8,
        #         "replacing_mutation_probability": 0.5,
        #         "sweep_mutation_probability": 0.4,
        #         "generations": 200
        #     }
        #     #TODO: remove this hack later
        #     ga = GAFactory.default().create_ga(silent=True,
        #                                              wf=wf,
        #                                              resource_manager=resource_manager,
        #                                              estimator=estimator,
        #                                              ga_params=params)
        #     current_schedule = Schedule({node: [] for node in resource_manager.get_nodes()})
        #     result = ga(current_schedule, None)
        #     init_schedule = result[0][2]
        # else:
        #     init_schedule = bundle.ga_schedule

        init_schedule = bundle.ga_schedule



        ga_machine = GAExecutor(wf,
                            resource_manager,
                            estimator,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            initial_schedule=init_schedule)

        ga_machine.init()
        ga_machine.run()

        resulted_schedule = ga_machine.current_schedule
        (makespan, vl1, vl2) = self.extract_result(resulted_schedule, is_silent, wf)
        print("GA: " + str(makespan))
        return makespan

    @staticmethod
    def single_run():
        obj = GAExecutorRunner()
        wf_name = 'CyberShake_30'
        reliability = 0.6
        obj.main(reliability, False, wf_name)

