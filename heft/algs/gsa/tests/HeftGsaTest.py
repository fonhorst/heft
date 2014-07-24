import unittest

from deap.base import Toolbox
from deap.tools.support import Statistics, Logbook
import numpy
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from heft.algs.gsa.SimpleGsaScheme import run_gsa
from heft.algs.gsa.heftbasedoperators import generate, force_vector_matrix, velocity_and_position, fitness
from heft.algs.gsa.operators import G, Kbest
from heft.algs.heft.HeftHelper import HeftHelper
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg


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

        toolbox.register("force_vector_matrix", force_vector_matrix, rm)
        toolbox.register("velocity_and_position", velocity_and_position, _wf, rm, estimator)
        toolbox.register("G", G)
        toolbox.register("kbest", Kbest)

        statistics = Statistics()
        statistics.register("min", lambda pop: numpy.min([p.fitness.mofit for p in pop]))
        statistics.register("avr", lambda pop: numpy.average([p.fitness.mofit for p in pop]))
        statistics.register("max", lambda pop: numpy.max([p.fitness.mofit for p in pop]))
        statistics.register("std", lambda pop: numpy.std([p.fitness.mofit for p in pop]))

        logbook = Logbook()
        logbook.header = ("gen", "G", "kbest", "min", "avr", "max", "std")

        pop_size = 10
        iter_number = 20
        kbest = pop_size
        ginit = 1

        final_pop = run_gsa(toolbox, statistics, logbook, pop_size, iter_number, kbest, ginit)

        best = min(final_pop, key=lambda x: toolbox.fitness(x).mofit)
        solution = {MAPPING_SPECIE: list(zip(sorted_tasks, best)), ORDERING_SPECIE: sorted_tasks}
        schedule = build_schedule(_wf, estimator, rm, solution)
        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        print("Final makespan: {0}".format(makespan))

        pass
    pass
if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(HeftGsaTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
