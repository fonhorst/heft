import unittest
from GA.DEAPGA.coevolution.cga import run_cooperative_ga
from GA.DEAPGA.coevolution.operators import VMResourceManager, create_simple_toolbox
from core.concrete_realization import ExperimentEstimator
from environment.Utility import Utility


class CoevoluitionTest(unittest.TestCase):

    def mapping_and_ordering(self):
        wf_path = ""
        wf_name = ""
        slots_count = 10
        wf = Utility.readWorkflow(wf_path, wf_name)
        manager = VMResourceManager(slots_count)
        estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)
        toolbox = create_simple_toolbox(wf, estimator, manager,
                      pop_size=10,
                      interact_individuals_count=22,
                      generations=10,
                      mutation_probability=0.5,
                      crossover_probability=0.8)
        run_cooperative_ga(toolbox)
        pass

    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(CoevoluitionTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
