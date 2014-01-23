import unittest
from GA.GenetisScheduler import StaticGeneticScheduler
from environment.Resource import ResourceGenerator
from environment.Utility import Utility
from reschedulingheft.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from reschedulingheft.simple_heft import StaticHeftPlanner
from tests.ProfilingTestCase import ProfilingTestCase


class GAvsHEFTTest(ProfilingTestCase):
    ideal_flops = 2
    def wrap(self):
        ##Preparing

        dax1 = '..\\resources\\CyberShake_30.xml'
        ##dax1 = '..\\resources\\Montage_100.xml'

        wf_start_id_1 = "00"
        task_postfix_id_1 = "00"

        deadline_1 = 1000

        wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
        rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=4,
                                 max_node_count=4)
                                 ##min_flops=20,
                                ## max_flops=20)
        resources = rgen.generate()
        transferMx = rgen.generateTransferMatrix(resources)
        estimator = ExperimentEstimator(transferMx, self.ideal_flops)

        planner = StaticHeftPlanner()
        planner.estimator = estimator
        planner.resource_manager = ExperimentResourceManager(resources)
        planner.workflows = [wf]

        schedule_heft = planner.schedule()
        heft_makespan = Utility.get_the_last_time(schedule_heft)
        print("heft_makespan: " + str(heft_makespan))

        scheduler = StaticGeneticScheduler(estimator, wf, resources,
                                           generation_count=700,
                                           population_size=100,
                                           crossover_probability=0.9,
                                           mutation_probability=0.3)
        schedule_ga = scheduler.schedule()
        ga_makespan = Utility.get_the_last_time(schedule_ga)
        print("ga_makespan: " + str(ga_makespan))



        k = 0

        ##assert(Utility.validateNodesSeq(schedule) is True)
        ##assert(Utility.validateParentsAndChildren(schedule, wf) is True)

if __name__ == '__main__':
    unittest.main()
