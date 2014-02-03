__author__ = 'Николай'

import unittest
from unittest import TestCase
from environment.Utility import Utility
from core.simple_heft import StaticHeftPlanner
from environment.Resource import ResourceGenerator
from core.concrete_realization import ExperimentEstimator
from core.concrete_realization import ExperimentResourceManager
import cProfile, pstats, io


def wrap():
    ##Preparing
    ut = Utility()

    dax1 = '..\\resources\\Montage_25.xml'
    dax2 = '..\\resources\\CyberShake_30.xml'

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"

    wf_start_id_2 = "10"
    task_postfix_id_2 = "10"

    deadline_1 = 1000
    deadline_2 = 3000

    pipeline_1 = ut.generateUrgentPipeline(dax1,
                                           wf_start_id_1,
                                           task_postfix_id_1,
                                           deadline_1)

    ##pipeline_2 = ut.generateUrgentPipeline(dax2,
    ##                                       wf_start_id_2,
    ##                                       task_postfix_id_2,
    ##                                       deadline_2)
    common_pipeline = pipeline_1 ## + pipeline_2

    rgen = ResourceGenerator()
    resources = rgen.generate()
    transferMx = rgen.generateTransferMatrix(resources)

    estimator = ExperimentEstimator(transferMx)

    planner = StaticHeftPlanner()
    planner.estimator = estimator
    planner.resource_manager = ExperimentResourceManager(resources)
    planner.workflows = common_pipeline

    schedule = planner.schedule()

    print("planner.global_count: " + str(planner.global_count))

class QuantExperiment(TestCase):

    def test_quant(self):

        ##cProfile.run('wrap()')

        pr = cProfile.Profile()
        pr.enable()
        wrap()
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        self.assertTrue(True)
if __name__ == '__main__':
    unittest.main()
