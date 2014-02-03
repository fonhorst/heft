import unittest
from GA.GenetisScheduler import StaticGeneticScheduler
from environment.Resource import ResourceGenerator
from environment.Utility import Utility
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from core.simple_heft import StaticHeftPlanner
from tests.ProfilingTestCase import ProfilingTestCase


class GAvsHEFTTest(ProfilingTestCase):
    IDEAL_FLOPS = 25
    T1 = 40
    T2 = 30
    T3 = 20
    T4 = 10

    T1_PIECE = 0.1
    T2_PIECE = 0.25
    T3_PIECE = 0.35
    T4_PIECE = 0.2

    ##TODO: remake it later

    def experiment(self):
         ##Preparing

        dax1 = '..\\resources\\CyberShake_30.xml'

        wf_start_id_1 = "00"
        task_postfix_id_1 = "00"

        deadline_1 = 1000

        wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)
        rgen = ResourceGenerator(min_res_count=1,
                                 max_res_count=1,
                                 min_node_count=10,
                                 max_node_count=10,
                                 min_flops=20,
                                 max_flops=20)
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

    def wrap(self):
        ##array of counts of nodes in a dedicated resource
        node_counts = [4, 8, 12, 16, 20]
        distribution = [(self.T1, self.T1_PIECE),
                        (self.T2, self.T2_PIECE),
                        (self.T3, self.T3_PIECE),
                        (self.T4, self.T4_PIECE)]

        for nc in node_counts:
            resource = generate_resource(nc, distribution)
            for
            experiment(resource)


if __name__ == '__main__':
    unittest.main()

