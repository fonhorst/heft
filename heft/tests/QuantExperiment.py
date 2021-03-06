import os
from heft.core.environment.ResourceGenerator import ResourceGenerator
from heft.core.environment.Utility import Utility

import unittest
from unittest import TestCase
from heft.algs.heft.simple_heft import StaticHeftPlanner
from heft.core.CommonComponents.ExperimentalManagers import ExperimentEstimator
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
import cProfile, pstats, io
from heft.settings import RESOURCES_PATH


def wrap():
    ##Preparing
    ut = Utility()

    wf_name = 'Montage_25'
    dax1 = os.path.join(RESOURCES_PATH,  wf_name + '.xml')
    #dax2 = '..\\resources\\CyberShake_30.xml'

    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"

    wf_start_id_2 = "10"
    task_postfix_id_2 = "10"

    deadline_1 = 1000
    deadline_2 = 3000

    pipeline_1 = ut.generateUrgentPipeline(dax1,
                                           wf_name,
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

    estimator = ExperimentEstimator(transferMx, 20)

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
