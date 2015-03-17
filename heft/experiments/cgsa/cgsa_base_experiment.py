from copy import deepcopy
import random
import os
from deap.base import Toolbox
from deap.tools import Statistics, Logbook
import numpy

from heft.algs.gsa.cgsa.cgsa_scheme import run_cgsa
from heft.algs.gsa.cgsa.operators import G, Kbest
from heft.algs.gsa.cgsa.ordering_mapping_operators import force, mapping_update, ordering_update, CompoundParticle, generate
from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.gsa.cgsa.ordering_operators import fitness, build_schedule
from heft.algs.gsa.cgsa.configuration_particle import config_generate, make_rm, configuration_update
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.experiments.common import AbstractExperiment
from heft.algs.common.utilities import logbooks_in_data, unzip_result, data_to_file
from heft.experiments.cga.utilities.common import repeat


class CgsaBaseExperiment(AbstractExperiment):
    @staticmethod
    def run(**kwargs):
        inst = CgsaBaseExperiment(**kwargs)
        return inst()

    def __init__(self, wf_name, W, C, G, GEN, N, KBEST, MAX_FLOPS, SUM_FLOPS):
        super().__init__(wf_name)

        self.W = W
        self.C = C
        self.G = G
        self.GEN = GEN
        self.N = N
        self.KBEST = KBEST
        self.MAX_FLOPS = MAX_FLOPS
        self.SUM_FLOPS = SUM_FLOPS
        pass

    def __call__(self):

        stats, logbook = self.stats(), self.logbook()
        _wf, rm, estimator = self.env()

        toolbox = self.toolbox()
        heft_schedule = self.heft_schedule()
        first_heft = ("Standart rm Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

        pop, log, best = run_cgsa(
            toolbox=toolbox,
            stats=stats,
            logbook=logbook,
            n=self.N,
            gen_curr=0, gen_step=self.GEN, initial_pop=None, kbest=self.KBEST, ginit=self.G,
            **{"w": self.W, "c": self.C, "max_flops": self.MAX_FLOPS, "flops_sum": self.SUM_FLOPS}
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
        #print(best[0][1].get_nodes())
        return best[1].values[0], log #makespan, log


    def toolbox(self):

        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()

        heft_particle = generate(_wf, rm, estimator, heft_schedule)

        heft_gen = lambda n: ([generate(_wf, rm, estimator) for _ in range(n-2)] + [deepcopy(heft_particle)] + [deepcopy(heft_particle)])
        #heft_gen = lambda n: ([generate(_wf, rm, estimator) for _ in range(n)])

        config_gen = lambda n: ([config_generate(self.SUM_FLOPS, self.MAX_FLOPS, self._ideal_flops, rm.get_nodes()) for _ in range(n-1)]
            + [config_generate(self.SUM_FLOPS, self.MAX_FLOPS, self._ideal_flops, rm.get_nodes(), [10, 15, 25, 30])])

        def componoud_update(w, c, p, min=-1, max=1):
            mapping_update(w, c, p.mapping)
            ordering_update(w, c, p.ordering, min=min, max=max)

        def config_update(w, c, p):
            configuration_update(w, c, p, self.MAX_FLOPS, self.SUM_FLOPS)

        def HEFT_solution():
            return (deepcopy(heft_particle), config_generate(self.SUM_FLOPS, self.MAX_FLOPS, self._ideal_flops, rm.get_nodes(), [10, 15, 25, 30]))

        def compound_force(p, pop, kbest, G):
            mapping_force = force(p.mapping, (p.mapping for p in pop), kbest, G)
            ordering_force = force(p.ordering, (p.ordering for p in pop), kbest, G)
            return (mapping_force, ordering_force)

        def configuration_force(p, pop, kbest, G):
            return force(p, pop, kbest, G)

        toolbox = Toolbox()
        toolbox.register("generate", heft_gen)
        toolbox.register("config_population", config_gen)
        toolbox.register("fitness", fitness, _wf, estimator)
        toolbox.register("update", componoud_update, self.W, self.C)
        toolbox.register("config_update", config_update, self.W, self.C)
        toolbox.register("standard", HEFT_solution)
        toolbox.register("estimate_force", compound_force)
        toolbox.register("estimate_config_force", configuration_force)
        toolbox.register("G", G)
        toolbox.register("kbest", Kbest)
        return toolbox
    pass

# Previous logbook
#logbook = Logbook()
#logbook.header = ["gen", "G", "kbest"] + stats.fields

if __name__ == "__main__":
    wf_list = ["Montage_25", "Montage_50", "Montage_75", "Montage_100", "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100"]
    wf = "CyberShake_100"
    path = "D:\eScience\Work\experiments\CGSA"
    pop_size = 20
    iter_number = 10
    kbest = 10
    ginit = 10
    W, C = 0.65, 0.35
    max_flops = 30
    flops_sum = 80
    exec_count = 1
#0.6599917494826476	0.3484509830252245	9	3
    results = []

    comment = ("w=" + str(W) + " c1=" + str(C) + " gen=" + str(iter_number)
               + " n=" + str(pop_size) + " max_flops=" + str(max_flops) + " flops_sum=" + str(flops_sum) + "small mass weight")
    best_result = 1000
    for iter in range(1):
        print(wf)
        #W = random.random()
        #C = random.random()
        #kbest = random.randint(1, 10)
        #ginit = random.randint(1, 10)
        #print(str(W) + "\t" + str(C) + "\t" + str(kbest) + "\t" + str(ginit))
        exp = CgsaBaseExperiment(wf_name=wf,
                                W=W, C=C, G=ginit,
                                GEN=iter_number, N=pop_size, KBEST=kbest, MAX_FLOPS=max_flops, SUM_FLOPS=flops_sum)
        result, logbooks = unzip_result(repeat(exp, exec_count))
        mean = numpy.mean(result)
        """
        if mean < best_result:
            best_result = mean
            data = logbooks_in_data(logbooks, False)
            data_to_file(os.path.join(path, (wf + "_cwgsa.txt")), iter_number, data, comment)
        """
        print(str(numpy.mean(result)) + ": \t" + str(result))
        results.append(numpy.mean(result))
    pass
    print(str(results))
