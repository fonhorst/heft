__author__ = 'Николай'

import unittest
from unittest import TestCase
from environment.Utility import Utility
from onlinedvmheft.simple_heft import StaticHeftPlanner
from environment.Resource import ResourceGenerator
from environment.ResourceManager import Estimator
from environment.ResourceManager import ResourceManager

class ExperimentEstimator(Estimator):
    def __init__(self, transferMx):
        self.transfer_matrix = transferMx

    ##get estimated time of running the task on the node
    def estimate_runtime(self, task, node):
        return task.runtime/node.flops

    ## estimate transfer time between node1 and node2 for data generated by the task
    def estimate_transfer_time(self, node1, node2, task1, task2):
        per_unit_of_time = self.transfer_matrix[node1.name][node2.name]
        ##TODO: it doesn't account non-task's input files. Add it later.
        def get_transfer_time(name):
            file = task1.output_files.get(name,None)
            return 0 if file is None else file.size
        return sum([ get_transfer_time(name) for (name,file) in task2.input_files.items()])/per_unit_of_time


    ## estimate probability of successful ending of the task on the node
    def estimate_reliability(self, task, node):
        pass

class ExperimentResourceManager(ResourceManager):
    def __init__(self,resources):
        self.resources = resources

    ##get all resources in the system
    def get_resources(self):
        return self.resources


class QuantExperiment(TestCase):
    def test_quant(self):
        ##Preparing
        ut = Utility()

        dax1 = 'C:\\WorkspaceDisk\\wfsim\\heft\\resources\\Montage_25.xml'
        dax2 = 'C:\\WorkspaceDisk\\wfsim\\heft\\resources\\CyberShake_30.xml'

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

        pipeline_2 = ut.generateUrgentPipeline(dax2,
                                               wf_start_id_2,
                                               task_postfix_id_2,
                                               deadline_2)
        common_pipeline = pipeline_1 + pipeline_2

        rgen = ResourceGenerator()
        resources = rgen.generate()
        transferMx = rgen.generateTransferMatrix(resources)

        estimator = ExperimentEstimator(transferMx)

        planner = StaticHeftPlanner()
        planner.estimator = estimator
        planner.resource_manager = ExperimentResourceManager(resources)
        planner.workflows = common_pipeline

        schedule = planner.schedule()

        ##TODO: fake run remake it later.
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
