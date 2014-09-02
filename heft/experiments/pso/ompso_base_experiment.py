from copy import deepcopy
from functools import partial
import random
from deap import tools
from deap.base import Toolbox
import numpy
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.ompso import run_ompso, fitness, generate, construct_solution, ordering_update
from heft.algs.pso.sdpso import run_pso, update, schedule_to_position
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE, ord_and_map
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.common import AbstractExperiment


class OmpsoBaseExperiment(AbstractExperiment):

    @staticmethod
    def run(**kwargs):
        inst = OmpsoBaseExperiment(**kwargs)
        return inst()

    def __init__(self, wf_name, W, C1, C2, GEN, N):
        super().__init__(wf_name)

        self.W = W
        self.C1 = C1
        self.C2 = C2
        self.GEN = GEN
        self.N = N
        pass

    def __call__(self):

        toolbox, stats, logbook = self.toolbox(), self.stats(), self.logbook()
        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()

        pop, log, best = run_ompso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, pop=None,
            N=self.N,
        )



        mapping, ordering = best.mapping.entity, best.ordering.entity
        solution = construct_solution(mapping, ordering)
        schedule = build_schedule(_wf, estimator, rm, solution)

        Utility.validate_static_schedule(_wf, schedule)

        makespan = Utility.makespan(schedule)
        print("Final makespan: {0}".format(makespan))
        print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
        return makespan

    def toolbox(self):
        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()

        heft_particle = generate(_wf, rm, estimator, heft_schedule)

        heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.0 else generate(_wf, rm, estimator) for _ in range(n)]

        toolbox = Toolbox()

        toolbox.register("generate", heft_gen)
        toolbox.register("fitness", fitness, _wf, rm, estimator)
        toolbox.register("pso_mapping", self._build_pso_mapping_alg())
        toolbox.register("pso_ordering", self._build_pso_ordering_alg())
        # toolbox.register("VNS", self._build_vns_alg())
        return toolbox

    def _check_fitness(self, x):
        if hasattr(x, "fitness") and x.fitness is not None and x.fitness.valid:
            return x.fitness
        raise ValueError("Fitness is not valid")

    def _build_pso_mapping_alg(self):
        toolbox = Toolbox()

        def population(n):
            raise Exception("This function mustn't have been called")

        def switching_update(w, c1, c2, p, best, pop):
            return update(w, c1, c2, p.mapping, best.mapping, pop)

        toolbox.register("population", population)
        toolbox.register("fitness", self._check_fitness)
        toolbox.register("update", switching_update)

        alg = partial(run_pso, toolbox,
                      w=self.W, c1=self.C1, c2=self.C2, n=self.N)
        return alg

    def _build_pso_ordering_alg(self):
        toolbox = Toolbox()

        def population(n):
            raise Exception("This function mustn't have been called")

        # def switching_update(w, c1, c2, p, best, pop):
        #     return update(w, c1, c2, p.mapping, best.mapping, pop)

        toolbox.register("population", population)
        toolbox.register("fitness", self._check_fitness)
        toolbox.register("update", ordering_update)

        alg = partial(run_pso, toolbox,
                      w=self.W, c1=self.C1, c2=self.C2, n=self.N)
        return alg

    def _build_vns_alg(self):
        return None
        # raise NotImplementedError()
    pass


if __name__ == "__main__":
    # result = repeat(do_exp, 1)
    exp = OmpsoBaseExperiment(wf_name="Montage_25",
                              W=0.1, C1=0.9, C2=0.6,
                              GEN=20, N=10)
    result = exp()
    print(result)
    pass

