import unittest
from GA.GenetisScheduler import StaticGeneticScheduler
from environment.Resource import ResourceGenerator
from environment.Utility import Utility
from reschedulingheft.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from reschedulingheft.simple_heft import StaticHeftPlanner
from tests.ProfilingTestCase import ProfilingTestCase


class GAvsHEFTTest(ProfilingTestCase):
    def wrap(self):
        ##Preparing

        dax1 = '..\\resources\\CyberShake_30.xml'

        wf_start_id_1 = "00"
        task_postfix_id_1 = "00"

        deadline_1 = 1000

        wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
        rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=5,
                                 max_node_count=5)
        resources = rgen.generate()
        transferMx = rgen.generateTransferMatrix(resources)
        estimator = ExperimentEstimator(transferMx)
        scheduler = StaticGeneticScheduler(estimator, wf, resources,
                                           generation_count=200,
                                           population_size=100,
                                           crossover_probability=0.9,
                                           mutation_probability=0.2)
        schedule_ga = scheduler.schedule()
        ga_makespan = Utility.get_the_last_time(schedule_ga)
        print("ga_makespan: " + str(ga_makespan))

        planner = StaticHeftPlanner()
        planner.estimator = estimator
        planner.resource_manager = ExperimentResourceManager(resources)
        planner.workflows = [wf]

        schedule_heft = planner.schedule()
        heft_makespan = Utility.get_the_last_time(schedule_heft)
        print("heft_makespan: " + str(heft_makespan))

        ##assert(Utility.validateNodesSeq(schedule) is True)
        ##assert(Utility.validateParentsAndChildren(schedule, wf) is True)

if __name__ == '__main__':
    unittest.main()
