from copy import deepcopy
import json
import unittest
import os
from heft.algs.common.individuals import ListBasedIndividual

from heft.algs.ga.coevolution.cga import run_cooperative_ga, Specie
from heft.algs.ga.coevolution.operators import build_schedule, default_config, MAPPING_SPECIE, ORDERING_SPECIE
from heft.core.CommonComponents.ExperimentalManagers import ExperimentEstimator, ExperimentResourceManager
from heft.core.environment.ResourceGenerator import ResourceGenerator
from heft.core.environment.Utility import wf as WF
from heft.core.environment.ResourceManager import ScheduleItem
from heft.experiments.cga.utilities.common import build_ms_ideal_ind
from heft.experiments.comparison_experiments.common.ExecutorRunner import ExecutorRunner
from heft.settings import TEMP_PATH


class CoevolutionTest(unittest.TestCase):

    def test_mapping_and_ordering(self):
        wf_name = "Montage_25"
        wf = WF(wf_name)
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
        wf_name = "Montage_25"
        wf = WF(wf_name)
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

    def test_predefined_init_pop(self):
        ## TODO: make loading from save file too
        wf_name = "Montage_25"
        _wf = WF(wf_name)
        manager = ExperimentResourceManager(ResourceGenerator.r([10, 15, 15, 25]))
        estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)

        config = default_config(_wf, manager, estimator)

        ## TODO: need to hide low level details
        import os
        path = os.path.join(TEMP_PATH, "cga_exp_for_example/5760f488-932e-4224-942f-1f5ac68709bf.json")
        with open(path, "r") as f:
            data = json.load(f)
        initial_pops = data["initial_pops"]

        mps = [ListBasedIndividual([tuple(g) for g in ind]) for ind in initial_pops[MAPPING_SPECIE]]
        os = [ListBasedIndividual(ind) for ind in initial_pops[ORDERING_SPECIE]]

        for s in config["species"]:
            if s.name == MAPPING_SPECIE:
                s.initialize = lambda ctx, size: mps
            elif s.name == ORDERING_SPECIE:
                 s.initialize = lambda ctx, size: os
            else:
                raise Exception("Unexpected specie: " + s.name)


        solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
        schedule = build_schedule(_wf, estimator, manager, solution)

        for k, items in schedule.mapping.items():
            for item in items:
                item.state = ScheduleItem.FINISHED
        ## TODO: refactor this
        ExecutorRunner.extract_result(schedule, True, _wf)
        pass

    def test_hall_of_fame(self):
        wf_name = "Montage_25"
        wf = WF(wf_name)
        manager = ExperimentResourceManager(ResourceGenerator.r([10, 15, 15, 25]))
        estimator = ExperimentEstimator(None, 20, 1.0, transfer_time=100)

        config = default_config(wf, manager, estimator)
        config["hall_of_fame_size"] = 3
        solution, pops, logbook, initial_pops = run_cooperative_ga(**config)
        schedule = build_schedule(wf, estimator, manager, solution)

        for k, items in schedule.mapping.items():
            for item in items:
                item.state = ScheduleItem.FINISHED
        ## TODO: refactor this
        ExecutorRunner.extract_result(schedule, True, wf)
        pass

    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(CoevolutionTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
