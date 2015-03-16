from copy import deepcopy
import random
from deap.base import Toolbox
import numpy

from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.pso.crdpso.ordering_operators import build_schedule, generate, ordering_update, fitness
from heft.algs.pso.crdpso.crdpso import run_cpso, init_rank_list
from heft.algs.pso.crdpso.mapping_operators import update as mapping_update
from heft.algs.pso.crdpso.configuration_particle import config_generate, make_rm, configuration_update
from heft.core.environment.Utility import Utility
from heft.experiments.aggregate_utilities import interval_statistics, interval_stat_string
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.common import AbstractExperiment

class CrdpsoBaseExperiment(AbstractExperiment):

    @staticmethod
    def run(**kwargs):
        inst = CrdpsoBaseExperiment(**kwargs)
        return inst()

    def __init__(self, wf_name, W, C1, C2, GEN, N, MAX_FLOPS, SUM_FLOPS):
        super().__init__(wf_name)

        self.W = W
        self.C1 = C1
        self.C2 = C2
        self.GEN = GEN
        self.N = N
        self.MAX_FLOPS = MAX_FLOPS
        self.SUM_FLOPS = SUM_FLOPS
        pass

    def __call__(self):

        stats, logbook = self.stats(), self.logbook()
        _wf, rm, estimator = self.env()

        toolbox = self.toolbox()
        heft_schedule = self.heft_schedule()
        first_heft = ("Standart rm Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

        pop, log, best = run_cpso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, initial_pop=None,
            w=self.W, c1=self.C1, c2=self.C2, n=self.N,
        )

        # change rm
        rm = make_rm(best[0][1])
        self._rm = rm
        self._resorces_set = best[0][1].get_nodes()
        #print("Resources: " + str(self._resorces_set))

        schedule = build_schedule(_wf, rm, estimator, best[0][0])

        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        #print("Final makespan: {0}".format(makespan))

        #print(first_heft)
        heft_schedule = run_heft(_wf, rm, estimator)
        #print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

        return makespan


    def toolbox(self):

        _wf, rm, estimator = self.env()

        rank_list = init_rank_list(_wf, rm, estimator)

        heft_schedule = self.heft_schedule()

        heft_particle = generate(_wf, rm, estimator, rank_list, heft_schedule)

        #heft_gen = lambda n: ([generate(_wf, rm, estimator, rank_list) for _ in range(n-1)] + [deepcopy(heft_particle)])
        heft_gen = lambda n: ([generate(_wf, rm, estimator, rank_list) for _ in range(n)])

        config_gen = lambda n: ([config_generate(self.SUM_FLOPS, self.MAX_FLOPS, self._ideal_flops, rm.get_nodes()) for _ in range(n)])

        def componoud_update(rank_l, w, c1, c2, p, best):
            mapping_update(w, c1, c2, p.mapping, best.mapping)
            ordering_update(w, c1, c2, p.ordering, best.ordering, rank_l)

        def config_update(w, c1, c2, p, best):
            configuration_update(w, c1, c2, p, best, self.MAX_FLOPS, self.SUM_FLOPS)

        toolbox = Toolbox()
        toolbox.register("population", heft_gen)
        toolbox.register("config_population", config_gen)
        toolbox.register("fitness", fitness, _wf, estimator)
        toolbox.register("update", componoud_update, rank_list)
        toolbox.register("config_update", config_update)
        return toolbox
    pass

if __name__ == "__main__":
    exp = CrdpsoBaseExperiment(wf_name="CyberShake_50",
                              W=0.5, C1=0.9, C2=0.6,
                              GEN=10, N=50, MAX_FLOPS=30, SUM_FLOPS=80)
    result = repeat(exp, 1)
    print(result)
    sts = interval_statistics(result)
    print("Statistics: {0}".format(interval_stat_string(sts)))
    print("Average: {0}".format(numpy.mean(result)))
    pass

