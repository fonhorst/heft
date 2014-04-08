from functools import partial
import unittest
from core.runners.ExecutorRunner import ExecutorsFactory


class TestExecsExecutor(unittest.TestCase):

    RELIABLE = 1
    LOW_RELIABILITY = 0.95
    DEFAULT_WF_NAME = 'CyberShake_30'


    def setUp(self):
        self.ga_func = partial(ExecutorsFactory.default().run_ga_executor,
                                is_silent=True,
                                wf_name=self.DEFAULT_WF_NAME,
                                logger=None,
                                with_ga_initial=True)
        self.heft_func = partial(ExecutorsFactory.default().run_heft_executor,
                                is_silent=True,
                                wf_name=self.DEFAULT_WF_NAME,
                                logger=None)
        self.gaheft_func = partial(ExecutorsFactory.default().run_gaheft_executor,
                                   is_silent=True,
                                   wf_name=self.DEFAULT_WF_NAME,
                                   logger=None,
                                   fixed_interval_for_ga=15,
                                   ga_params={
                                        "population": 50,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 10
                                    })
        self.cloudheft_func = partial(ExecutorsFactory.default().run_cloudheft_executor,
                                      is_silent=True,
                                      wf_name=self.DEFAULT_WF_NAME,
                                      logger=None,)
        self.oldpopga_func = partial(ExecutorsFactory.default().run_oldpop_executor,
                                     is_silent=True,
                                     wf_name=self.DEFAULT_WF_NAME,
                                     logger=None,
                                     key_for_save='by_5',
                                     task_id_to_fail="ID00005_000",
                                     fixed_interval_for_ga=15,
                                     ga_params={
                                        "population": 50,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 10
                                     })
        self.oldpopgaheft_func = partial(ExecutorsFactory.default().run_gaheftoldpop_executor,
                                     is_silent=True,
                                     wf_name=self.DEFAULT_WF_NAME,
                                     logger=None,
                                     key_for_save='test',
                                     task_id_to_fail="ID00005_000",
                                     fixed_interval_for_ga=15,
                                     ga_params={
                                        "population": 50,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 10
                                     })

        self.oldpopmpgaheft_func = partial(ExecutorsFactory.default().run_mpgaheftoldpop_executor,
                                     is_silent=True,
                                     wf_name=self.DEFAULT_WF_NAME,
                                     logger=None,
                                     key_for_save='test',
                                     task_id_to_fail="ID00005_000",
                                     fixed_interval_for_ga=15,
                                     migrCount=5,
                                     emigrant_selection=None,
                                     all_iters_count=10,
                                     ga_params={
                                        "population": 50,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 2
                                     })
        pass

    def _run(self, name):
        func = self.__getattribute__(name+"_func")
        res1 = func(reliability=self.RELIABLE)
        assert True
        res2 = func(reliability=self.LOW_RELIABILITY)
        assert True
        pass


    def test_ga_executor(self):
        self._run("ga")
        pass

    def test_heft_executor(self):
        self._run("heft")
        pass

    def test_gaheft_executor(self):
        self._run("gaheft")
        pass

    def test_cloudheft_executor(self):
        self._run("cloudheft")
        pass

    def test_oldpopga_executor(self):
        self._run("oldpopga")
        pass

    def test_oldpopgaheft_executor(self):
        self._run("oldpopgaheft")
        pass

    def test_oldpopmpgaheft_executor(self):
        self._run("oldpopmpgaheft")
        pass

    pass

if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestExecsExecutor)
    unittest.TextTestRunner(verbosity=2).run(suite)
