from copy import deepcopy
import random
from deap.base import Toolbox
import numpy

from heft.algs.pso.ompso import fitness, generate, build_schedule, ordering_update
from heft.algs.pso.sdpso import run_pso
from heft.algs.pso.sdpso import update as mapping_update
from heft.core.environment.Utility import Utility
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

        pop, log, best = run_pso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, initial_pop=None,
            w=self.W, c1=self.C1, c2=self.C2, n=self.N,
        )

        schedule = build_schedule(_wf, rm, estimator,  best)

        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        print("Final makespan: {0}".format(makespan))
        print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
        return makespan

    def toolbox(self):

        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()

        heft_particle = generate(_wf, rm, estimator, heft_schedule)

        heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.00 else generate(_wf, rm, estimator) for _ in range(n)]

        def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
            mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
            ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)

        toolbox = Toolbox()
        toolbox.register("population", heft_gen)
        toolbox.register("fitness", fitness, _wf, rm, estimator)
        toolbox.register("update", componoud_update)
        return toolbox


    pass


if __name__ == "__main__":
    exp = OmpsoBaseExperiment(wf_name="Montage_100",
                              W=0.1, C1=0.6, C2=0.2,
                              GEN=300, N=100)
    result = repeat(exp, 5)
    # result = exp()
    print(result)
    print("Average: {0}".format(numpy.mean(result)))
    pass

