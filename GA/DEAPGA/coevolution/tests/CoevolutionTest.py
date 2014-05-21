from copy import deepcopy
import unittest
from GA.DEAPGA.coevolution.cga import run_cooperative_ga, Specie, ListBasedIndividual
from GA.DEAPGA.coevolution.operators import build_schedule, default_config, MAPPING_SPECIE
from GA.DEAPGA.coevolution.utilities import build_ms_ideal_ind
from core.concrete_realization import ExperimentEstimator, ExperimentResourceManager
from core.runners.ExecutorRunner import ExecutorRunner
from environment.ResourceGenerator import ResourceGenerator
from environment.ResourceManager import ScheduleItem
from environment.Utility import Utility


class CoevolutionTest(unittest.TestCase):
    def test_mapping_and_ordering(self):
        wf_path = "../../../../resources/Montage_25.xml"
        wf_name = "Montage_25"
        wf = Utility.readWorkflow(wf_path, wf_name)
        manager = ExperimentResourceManager(ResourceGenerator.r([10, 15, 15, 25]))
        estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)

        config = default_config(wf, manager, estimator)
        solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
        schedule = build_schedule(wf, estimator, manager, solution)

        for k, items in schedule.mapping.items():
            for item in items:
                item.state = ScheduleItem.FINISHED
        ## TODO: refactor this
        ExecutorRunner.extract_result(schedule, True, wf)

        config_2 = deepcopy(config)
        config_2["interact_individuals_count"] = 4

        solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
        schedule = build_schedule(wf, estimator, manager, solution)

        for k, items in schedule.mapping.items():
            for item in items:
                item.state = ScheduleItem.FINISHED
        ## TODO: refactor this
        ExecutorRunner.extract_result(schedule, True, wf)
        pass

    def test_fixed_specie(self):
        wf_path = "../../../../resources/Montage_25.xml"
        wf_name = "Montage_25"
        wf = Utility.readWorkflow(wf_path, wf_name)
        manager = ExperimentResourceManager(ResourceGenerator.r([10, 15, 15, 25]))
        estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)

        config = default_config(wf, manager, estimator)

        ms_ideal_ind = build_ms_ideal_ind(wf, manager)
        config["species"][0] = Specie(name=MAPPING_SPECIE, fixed=True,
                                      representative_individual=ListBasedIndividual(ms_ideal_ind))

        solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
        schedule = build_schedule(wf, estimator, manager, solution)

        for k, items in schedule.mapping.items():
            for item in items:
                item.state = ScheduleItem.FINISHED
        ## TODO: refactor this
        ExecutorRunner.extract_result(schedule, True, wf)
        pass

    # def test_predefined_init_pop(self):
    #     wf_path = "../../../../resources/Montage_25.xml"
    #     wf_name = "Montage_25"
    #     wf = Utility.readWorkflow(wf_path, wf_name)
    #     manager = ExperimentResourceManager(ResourceGenerator.r([10, 15, 15, 25]))
    #     estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)
    #
    #     config = default_config(wf, manager, estimator)
    #
    #
    #
    #     for s in config["species"]:
    #         s.initialize = lambda ctx, size: initialize_from_predefined(ctx, s.name + "_initpop")
    #
    #     solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
    #     schedule = build_schedule(wf, estimator, manager, solution)
    #
    #     for k, items in schedule.mapping.items():
    #         for item in items:
    #             item.state = ScheduleItem.FINISHED
    #     ## TODO: refactor this
    #     ExecutorRunner.extract_result(schedule, True, wf)
    #     pass



    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(CoevolutionTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
