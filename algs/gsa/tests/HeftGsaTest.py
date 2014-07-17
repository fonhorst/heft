import unittest
from deap.base import Toolbox
from deap.tools.support import Statistics, Logbook
import numpy
from algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from algs.gsa.SimpleGsaScheme import run_gsa
from algs.gsa.heftbasedoperators import generate, force_vector_matrix, velocity_and_position, fitness
from algs.gsa.operators import G, Kbest
from algs.heft.HeftHelper import HeftHelper
from core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from core.environment.Utility import wf, Utility
from experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from core.environment.ResourceGenerator import ResourceGenerator as rg


class HeftGsaTest(unittest.TestCase):

    def test_fixed_ordering(self):
        _wf = wf("Montage_25")
        rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
        estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
        sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

        toolbox = Toolbox()
        toolbox.register("generate", generate, _wf, rm, estimator)
        toolbox.register("fitness", fitness, _wf, rm, estimator, sorted_tasks)

        toolbox.register("force_vector_matrix", force_vector_matrix)
        toolbox.register("velocity_and_position", velocity_and_position, _wf, rm, estimator)
        toolbox.register("G", G)
        toolbox.register("kbest", Kbest)

        statistics = Statistics()
        statistics.register("min", lambda pop: numpy.min([p.fitness for p in pop]))
        statistics.register("avr", lambda pop: numpy.average([p.fitness for p in pop]))
        statistics.register("max", lambda pop: numpy.max([p.fitness for p in pop]))
        statistics.register("std", lambda pop: numpy.std([p.fitness for p in pop]))

        logbook = Logbook()

        pop_size = 50
        iter_number = 200
        kbest = pop_size
        ginit = 1

        final_pop = run_gsa(toolbox, statistics, logbook, pop_size, iter_number, kbest, ginit)

        best = min(final_pop, key=lambda x: x.fitness)
        solution = {MAPPING_SPECIE: best, ORDERING_SPECIE: sorted_tasks}
        schedule = build_schedule(_wf, rm, estimator, solution)
        Utility.validate_static_schedule(schedule)
        makespan = Utility.makespan(schedule)
        print("Final makespan: {0}".format(makespan))

        pass
    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(HeftGsaTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
