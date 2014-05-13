import unittest
from GA.DEAPGA.coevolution.cga import run_cooperative_ga
from GA.DEAPGA.coevolution.operators import VMResourceManager, create_simple_toolbox, build_schedule
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from core.runners.ExecutorRunner import ExecutorRunner
from environment.ResourceGenerator import ResourceGenerator
from environment.ResourceManager import ScheduleItem
from environment.Utility import Utility


class CoevolutionTest(unittest.TestCase):
    def test_mapping_and_ordering(self):
        wf_path = "D:/wspace/heft/resources/Montage_25.xml"
        wf_name = "Montage_25"
        wf = Utility.readWorkflow(wf_path, wf_name)
        manager = ExperimentResourceManager(ResourceGenerator.r([10, 15, 15, 25]))
        estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)

        toolbox = create_simple_toolbox(wf, estimator, manager,
                                        pop_size=10,
                                        interact_individuals_count=22,
                                        generations=5,
                                        mutation_probability=0.5,
                                        crossover_probability=0.8)
        solution = run_cooperative_ga(toolbox)
        schedule = build_schedule(wf, estimator, manager, solution)

        for k, items in schedule.mapping.items():
            for item in items:
                item.state = ScheduleItem.FINISHED
        ## TODO: refactor this
        ExecutorRunner.extract_result(schedule, True, wf)
        pass

    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(CoevolutionTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
