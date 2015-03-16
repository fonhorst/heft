from functools import partial
import json
import os
import unittest
import shutil

from heft.experiments.comparison_experiments.common.ExecutorRunner import ExecutorsFactory
from heft.experiments.comparison_experiments.common.ComparisonBase import ComparisonUtility as CU


## This test aims to validate all stat info written to file during
## a simulation process.
class TestGeneratedDataExecutor(unittest.TestCase):

    RELIABLE = 1
    LOW_RELIABILITY = 0.95
    DEFAULT_WF_NAME = 'Montage_25'



    def setUp(self):
        all_iters_count = 30
        self.mixed_mpgaheftoldpop_func = partial(ExecutorsFactory.default().run_mpgaheftoldpop_executor,
                                         # for this experiment it doesn't matter at all
                                         reliability=self.RELIABLE,
                                         is_silent=True,
                                         wf_name=self.DEFAULT_WF_NAME,
                                         logger=None,
                                         key_for_save='small_run',
                                         #task_id_to_fail="ID00005_000",
                                         #task_id_to_fail=tsk,
                                         #save_path=,
                                         fixed_interval_for_ga=15,
                                         migrCount=5,
                                         emigrant_selection=None,
                                         all_iters_count=all_iters_count,
                                         ga_params={
                                            "population": 10,
                                            "crossover_probability": 0.8,
                                            "replacing_mutation_probability": 0.5,
                                            "sweep_mutation_probability": 0.4,
                                            "generations": 5
                                         }, mixed_init_pop=True)

        def check_logbook(x):
            #all_iters = [i for i in range(0, all_iters_count)]
            for rec in x:
                assert "iter" in rec, "iter isn't presented in record"
                assert "worst" in rec, "worst isn't presented for iter {0}".format(rec["iter"])
                assert "best" in rec, "best isn't presented for iter {0}".format(rec["iter"])
                assert "avr" in rec, "avr isn't presented for iter {0}".format(rec["iter"])
                #all_iters.remove(rec["iter"])
                pass
            #assert len(all_iters) == 0, "Next iters wasn't be founded in results: {0}".format(all_iters)
            return True

        run_spec = {
                    "iter": lambda x: x is None or x < all_iters_count,
                    "makespan": lambda x: True,
                    "pop_aggr": check_logbook
        }
        self.stat_specification = {
                "wf_name": lambda x: x == self.DEFAULT_WF_NAME,
                "event_name": lambda x: x == "NodeUp" or x == "NodeFailed" or x == "",
                # p for param
                "task_id": lambda x: len(x) > 0,
                "with_old_pop": run_spec,
                "with_random": run_spec
        }

        pass

    def _check_spec(self, data, spec):
        for key, checker in spec.items():
            assert key in data, "Data doesn't contain key from spec: {0}".format(key)
            if isinstance(data[key], dict):
                self._check_spec(data[key], spec[key])
            else:
                assert checker(data[key]), "Checking was failed for key: {0}".format(key)
            pass
        pass


    def test_mpgaheftoldpop_mixed_init_pop_executor(self):
        TEST_SANDBOX_DIRECTORY = "../../results/mixed_mpgaheftoldpop_[{0}]_[{1}]/".format(CU.cur_time(), CU.uuid())
        tsks_list = ["ID00000_000", "ID00005_000", "ID00010_000", "ID00015_000", "ID00020_000"]
        for tsk in tsks_list:
            # run executor
            self.mixed_mpgaheftoldpop_func(save_path=TEST_SANDBOX_DIRECTORY, task_id_to_fail=tsk)
            pass

        # check sandbox
        assert os.path.exists(TEST_SANDBOX_DIRECTORY), "Sandbox wasn't created"

        # check generated files
        generated_files = [TEST_SANDBOX_DIRECTORY + entry for entry in os.listdir(TEST_SANDBOX_DIRECTORY) if os.path.isfile(TEST_SANDBOX_DIRECTORY + entry)]
        assert len(generated_files) == len(tsks_list), "Count of generated files( {0} ) after simulations doesn't match assumed( {1} )".format(len(generated_files), len(tsks_list))

        # check content of generated files
        for file in generated_files:
            with open(file, "r") as f:
                data = json.load(f)
                for d in data:
                    self._check_spec(d, self.stat_specification)
                    #TODO: there is possibility to get several results in one file, but for now we don't consider it
                    tsks_list.remove(d["task_id"])
                    pass
            pass
        assert len(tsks_list) == 0, "results for next tasks were not be found {0}".format(tsks_list)


        shutil.rmtree(TEST_SANDBOX_DIRECTORY)
        pass

    pass

if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGeneratedDataExecutor)
    unittest.TextTestRunner(verbosity=2).run(suite)
