from subprocess import call
import unittest
from GA.DEAPGA.GAImplementation.GAImplementation import Params
from core.comparisons.StopRescheduling import GaExecutorExample


class TestGaExecutor(unittest.TestCase):
    def setUp(self):
        self.example_executor = GaExecutorExample()
        self.wf_name = "Montage_50"
        self.ga_params = Params(20, 400, 0.8, 0.5, 0.4, 100)
        pass

    # def test_without_errors(self):
    #     ##here mustn't be any exception
    #     makespan = self.example_executor.main(1, True, self.wf_name, self.ga_params)
    #     pass

    def test_with_errors(self):
        ## here mustn't be any exception
        makespan = self.example_executor.main(0.95, True, self.wf_name, self.ga_params, key_for_save='11_03_14_16_15_45')
        pass

    pass

if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGaExecutor)
    unittest.TextTestRunner(verbosity=2).run(suite)