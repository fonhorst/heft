from copy import deepcopy
import random
from deap.base import Toolbox
import numpy
from heft.algs.gsa.SimpleGsaScheme import run_gsa
from heft.algs.gsa.operators import G, Kbest
from heft.algs.gsa.setbasedoperators import velocity_and_position, force_vector_matrix
from heft.algs.pso.ordering_operators import generate, ordering_update, fitness

from heft.algs.pso.sdpso import run_pso
from heft.algs.pso.sdpso import update as mapping_update
from heft.core.environment.Utility import Utility
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.common import AbstractExperiment


class OmgsaBaseExperiment(AbstractExperiment):

    @staticmethod
    def run(**kwargs):
        inst = OmgsaBaseExperiment(**kwargs)
        return inst()

    def __init__(self, wf_name, KBEST, G, GEN, N):
        super().__init__(wf_name)

        self.KBEST = KBEST
        self.G = G
        self.GEN = GEN
        self.N = N
        pass

    def __call__(self):

        toolbox, stats, logbook = self.toolbox(), self.stats(), self.logbook()
        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()

        pop, log, best = run_gsa(
            toolbox=toolbox,
            logbook=logbook,
            statistics=stats,
            pop_size=self.N,
            iter_number=self.GEN,
            kbest=self.KBEST,
            ginit=self.G
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

        def compound_force_vector_matrix():
            raise NotImplementedError()

        def compound_velocity_and_postion():
            raise NotImplementedError()


        toolbox = Toolbox()
        toolbox.register("generate", heft_gen)
        toolbox.register("fitness", fitness, _wf, rm, estimator, sorted_tasks)
        toolbox.register("force_vector_matrix", compound_force_vector_matrix)
        toolbox.register("velocity_and_position", compound_velocity_and_postion, beta=0.0)
        toolbox.register("G", G)
        toolbox.register("kbest", Kbest)
        return toolbox


    pass


if __name__ == "__main__":
    exp = OmpsoBaseExperiment(wf_name="Montage_100",
                              W=0.1, C1=0.6, C2=0.2,
                              GEN=300, N=100)
    # result = repeat(exp, 5)
    result = exp()
    print(result)
    print("Average: {0}".format(numpy.mean(result)))
    pass


