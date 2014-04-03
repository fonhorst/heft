import unittest
from core.comparisons.StopRescheduling import GaOldPopExecutorRunner


class TestGaExecutor(unittest.TestCase):
    def setUp(self):
        self.example_executor = GaOldPopExecutorRunner()
        self.wf_name = "Montage_50"
        self.ga_params = {
            "population": 50,
            "crossover_probability": 0.8,
            "replacing_mutation_probability": 0.5,
            "sweep_mutation_probability": 0.4,
            "generations": 500
        }
        pass

    # def test_without_errors(self):
    #     ##here mustn't be any exception
    #     makespan = self.example_executor.main(1, True, self.wf_name, self.ga_params)
    #     pass

    def test_with_errors(self):
        # ids = ["ID00016_000"]
        # ids = ["ID00005_000", "ID00010_000", "ID00015_000",
        #        "ID00020_000", "ID00025_000", "ID00030_000",
        #        "ID00035_000", "ID00040_000", "ID00045_000",
        #        "ID00049_000"]
        ## here mustn't be any exception
        # ids = ['ID00005_000']
        # ids = ['ID00010_000']
        # ids = ['ID00015_000']
        # ids = ['ID00005_000', 'ID00010_000', 'ID00015_000']
        ids = ['ID00020_000', 'ID00025_000']
        # ids = ['ID00030_000', 'ID00035_000']
        # ids = ['ID00040_000', 'ID00045_000', 'ID00049_000']
        print("Ids:" + str(ids))
        for i in range(10):
            for id in ids:
                makespan = self.example_executor.main(0.95, False, self.wf_name, self.ga_params,
                                                      key_for_save='by_5',
                                                      task_id_to_fail=id)
                pass
            pass
        pass

    pass

if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGaExecutor)
    unittest.TextTestRunner(verbosity=2).run(suite)