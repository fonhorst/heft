from copy import deepcopy
from functools import partial
import os
import unittest
from unittest.case import TestCase
import shutil

from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_pfpso, create_old_ga, create_pfgsa, \
    create_pfmpga, create_pso_alg, create_pso_cleaner, create_schedule_to_pso_chromosome_converter, \
    create_schedule_to_gsa_chromosome_converter, create_gsa_alg, create_ga_cleaner, \
    create_schedule_to_ga_chromosome_converter, create_old_pfmpga
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_gaheft_exp, do_triple_island_exp
from heft.experiments.comparison_experiments.gaheft_series.utilities import EXAMPLE_BASE_PARAMS, \
    changing_reliability_run, SaveToDirectory
from heft.settings import TEMP_PATH, TEST_DIRECTORY_NAME
from heft.algs.pso.ordering_operators import generate as pso_generate
from heft.algs.gsa.ordering_mapping_operators import generate as gsa_generate
from heft.experiments.comparison_experiments.gaheft_series.utilities import ga_generate



TEST_BASE_PARAMS = deepcopy(EXAMPLE_BASE_PARAMS)


class GaheftSeriesTest(TestCase):

    #raise NotImplementedError("Broken test. It is needed to rewrite saving to directory")
    RELIABILITY = [0.95]
    REPEAT_COUNT = 100
    WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
    INDIVIDUALS_COUNTS = [20]


    def setUp(self):
        self.save_path = os.path.join(TEMP_PATH, TEST_DIRECTORY_NAME)

    def tearDown(self):
        shutil.rmtree(self.save_path)
        pass


    def test_gaheft_for_pso(self):
        """
        If there are not exceptions, that means all is ok
        """
        params = deepcopy(TEST_BASE_PARAMS)
        params["experiment_name"] = "test_gaheft_for_pso"
        params["alg_name"] = "pso"
        params["alg_params"] = {
            "w": 0.1,
            "c1": 0.6,
            "c2": 0.2,
            "n": 50,
            "gen_curr": 0,
            "gen_step": 10,
        }

        saver = SaveToDirectory(self.save_path, params["experiment_name"])
        pso_exp = partial(do_gaheft_exp, alg_builder=create_pfpso)
        pso_exp = saver(pso_exp)
        changing_reliability_run(pso_exp, self.RELIABILITY, self.INDIVIDUALS_COUNTS, self.REPEAT_COUNT, self.WF_NAMES, params, is_debug=True)
        pass

    def test_gaheft_for_ga(self):
        params = deepcopy(TEST_BASE_PARAMS)
        params["experiment_name"] = "test_gaheft_for_ga"
        params["alg_name"] = "ga"
        params["alg_params"] = {
            "kbest": 5,
            "n": 10,
            "cxpb": 0.3,  # 0.8
            "mutpb": 0.1,  # 0.5
            "sweepmutpb": 0.3,  # 0.4
            "gen_curr": 0,
            "gen_step": 10,
            "is_silent": True
        }

        saver = SaveToDirectory(self.save_path, params["experiment_name"])
        pso_exp = partial(do_gaheft_exp, alg_builder=create_old_ga)
        pso_exp = saver(pso_exp)
        changing_reliability_run(pso_exp, self.RELIABILITY, self.INDIVIDUALS_COUNTS, self.REPEAT_COUNT, self.WF_NAMES, params, is_debug=True)
        pass

    def test_gaheft_for_gsa(self):
        params = deepcopy(TEST_BASE_PARAMS)
        params["experiment_name"] = "test_gaheft_for_gsa"
        params["alg_name"] = "gsa"
        params["alg_params"] = {
            "w": 0.2,
            "c": 0.5,
            "n": 10,
            "gen_curr": 0,
            "gen_step": 10,
            "ginit": 10

        }

        saver = SaveToDirectory(self.save_path, params["experiment_name"])
        pso_exp = partial(do_gaheft_exp, alg_builder=create_pfgsa)
        pso_exp = saver(pso_exp)
        changing_reliability_run(pso_exp, self.RELIABILITY, self.INDIVIDUALS_COUNTS, self.REPEAT_COUNT, self.WF_NAMES, params, is_debug=True)
        pass

    def test_migaheft_for_pso(self):
        """
        If there are not exceptions, that means all is ok
        """
        params = deepcopy(TEST_BASE_PARAMS)
        params["experiment_name"] = "test_migaheft_for_pso"
        params["alg_name"] = "pso"
        params["alg_params"] = {
            "w": 0.1,
            "c1": 0.6,
            "c2": 0.2,
            "n": 10,#50,
            ## param for init run
            "gen_curr": 0,
            ## param for init run
            "gen_step": 30,#300,

            "is_silent": True,
            "migrCount": 5,
            #"all_iters_count": 300,
            #"merged_pop_iters": 100,
            "generations_count_before_merge": 10,#200
            "generations_count_after_merge": 5,#100
            "migrationInterval": 5,

        }

        saver = SaveToDirectory(self.save_path, params["experiment_name"])
        pso_exp = partial(do_triple_island_exp, alg_builder=partial(create_pfmpga, algorithm=create_pso_alg, generate_func=pso_generate),
                          chromosome_cleaner_builder=create_pso_cleaner,
                          schedule_to_chromosome_converter_builder=create_schedule_to_pso_chromosome_converter)
        pso_exp = saver(pso_exp)
        changing_reliability_run(pso_exp, self.RELIABILITY, self.INDIVIDUALS_COUNTS, self.REPEAT_COUNT, self.WF_NAMES, params, is_debug=True)
        pass

    def test_migaheft_for_gsa(self):
        """
        If there are not exceptions, that means all is ok
        """
        params = deepcopy(TEST_BASE_PARAMS)
        params["experiment_name"] = "test_migaheft_for_gsa"
        params["alg_name"] = "gsa"
        params["alg_params"] = {
            "w": 0.2,
            "c": 0.5,
            "n": 10,#50,
            "ginit": 10,
            "kbest": 10,
            ## param for init run
            "gen_curr": 0,
            ## param for init run
            "gen_step": 10,#300,

            "is_silent": True,
            "migrCount": 5,
            #"all_iters_count": 300,
            #"merged_pop_iters": 100,
            "generations_count_before_merge": 20,
            "generations_count_after_merge": 10,
            "migrationInterval": 10,

        }

        saver = SaveToDirectory(self.save_path, params["experiment_name"])
        pso_exp = partial(do_triple_island_exp, alg_builder=partial(create_pfmpga, algorithm=create_gsa_alg, generate_func=gsa_generate),
                          chromosome_cleaner_builder=create_pso_cleaner,
                          schedule_to_chromosome_converter_builder=create_schedule_to_gsa_chromosome_converter)
        pso_exp = saver(pso_exp)
        changing_reliability_run(pso_exp, self.RELIABILITY, self.INDIVIDUALS_COUNTS, self.REPEAT_COUNT, self.WF_NAMES, params, is_debug=True)
        pass

    def test_migaheft_for_ga(self):
         params = deepcopy(TEST_BASE_PARAMS)
         params["experiment_name"] = "test_migaheft_for_ga"
         params["alg_name"] = "ga"
         params["alg_params"] = {
           "kbest": 5,
            "n": 10,#50,
            "cxpb": 0.3,  # 0.8
            "mutpb": 0.1,  # 0.5
            "sweepmutpb": 0.3,  # 0.4
            "gen_curr": 0,
            "gen_step": 10,#300,
            "is_silent": True,
            "merged_pop_iters": 10,
            "migrCount": 5,
            "all_iters_count": 30
            }
         saver = SaveToDirectory(self.save_path, params["experiment_name"])
         pso_exp = partial(do_triple_island_exp, alg_builder=create_old_pfmpga,
                           chromosome_cleaner_builder=create_ga_cleaner,
                           schedule_to_chromosome_converter_builder=create_schedule_to_ga_chromosome_converter)
         pso_exp = saver(pso_exp)
         changing_reliability_run(pso_exp, self.RELIABILITY, self.INDIVIDUALS_COUNTS, self.REPEAT_COUNT, self.WF_NAMES, params, is_debug=True)
         pass




    pass

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(GaheftSeriesTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
