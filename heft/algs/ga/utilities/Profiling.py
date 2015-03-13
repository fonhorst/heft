from heft.algs.ga.GAImplementation.GAImpl import GAFactory
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment import ResourceGenerator
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import profile_decorator, wf


_wf = wf("Montage_100")
resources = ResourceGenerator.r([10, 15, 25, 30])
resource_manager = ExperimentResourceManager(resources)
estimator = ExperimentEstimator(None, ideal_flops=20, reliability=1.0, transfer_time=100)

ga = GAFactory.default().create_ga(silent=True,
                              wf=_wf,
                              resource_manager=resource_manager,
                              estimator=estimator,
                              ga_params={
                                        "population": 10,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 20
                              })

@profile_decorator
def fnc():
    empty_schedule = Schedule({node: [] for node in resource_manager.get_nodes()})
    res = ga(empty_schedule, None)
    print(res)
    pass

if __name__ == "__main__":
    fnc()


