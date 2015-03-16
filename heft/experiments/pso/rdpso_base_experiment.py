from copy import deepcopy
import random
from deap.base import Toolbox
import numpy
from heft.algs.pso.rdpso.ordering_operators import build_schedule, generate, ordering_update, fitness

from heft.algs.pso.rdpso.rdpso import run_pso, initMapMatrix, initRankList, filterList
from heft.algs.pso.rdpso.mapping_operators import update as mapping_update
from heft.core.environment.Utility import Utility
from heft.experiments.aggregate_utilities import interval_statistics, interval_stat_string
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.common import AbstractExperiment
from heft.algs.heft.HeftHelper import HeftHelper


class RdpsoBaseExperiment(AbstractExperiment):

    @staticmethod
    def run(**kwargs):
        inst = RdpsoBaseExperiment(**kwargs)
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

        stats, logbook = self.stats(), self.logbook()
        _wf, rm, estimator = self.env()
        estimator.transfer_time = 500
        heft_schedule = self.heft_schedule()

        wf_dag = HeftHelper.convert_to_parent_children_map(_wf)
        jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
        nodes = rm.get_nodes()
        mapMatrix = initMapMatrix(jobs, nodes, estimator)
        rankList = initRankList(wf_dag, nodes, estimator)
        ordFilter = filterList(_wf)

        toolbox = self.toolbox(mapMatrix, rankList, ordFilter)

        pop, log, best = run_pso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, initial_pop=None,
            w=self.W, c1=self.C1, c2=self.C2, n=self.N, rm=rm, wf=_wf, estimator=estimator, mapMatrix=mapMatrix, rankList=rankList, ordFilter=ordFilter,
        )
        #print(str(best.fitness))
        schedule = build_schedule(_wf, rm, estimator,  best, mapMatrix, rankList, ordFilter)
        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        if makespan > best.fitness.values[0]:
            print("DANGER!!!!!!!!!!!!!!!!!")
        print("Final makespan: {0}".format(makespan))
        print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
        return makespan

    def toolbox(self, mapMatrix, rankList, ordFilter):

        _wf, rm, estimator = self.env()
        estimator.transfer_time = 500
        heft_schedule = self.heft_schedule()



        heft_particle = generate(_wf, rm, estimator, mapMatrix, rankList, ordFilter, heft_schedule)
        heft_gen = lambda n: ([deepcopy(heft_particle) if random.random() > 1.00 else generate(_wf, rm, estimator, mapMatrix, rankList, ordFilter) for _ in range(n-1)] + [deepcopy(heft_particle)])
        #heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.00 else generate(_wf, rm, estimator, mapMatrix, rankList, ordFilter) for _ in range(n)]

        def componoud_update(w, c1, c2, p, best, pop):
            #doMap = random.random()
            #if doMap < 0.1:
            mapping_update(w, c1, c2, p.mapping, best.mapping, pop)
            #else:
            ordering_update(w, c1, c2, p.ordering, best.ordering, pop)

        toolbox = Toolbox()
        toolbox.register("population", heft_gen)
        toolbox.register("fitness", fitness, _wf, rm, estimator)
        toolbox.register("update", componoud_update)
        return toolbox


    pass


if __name__ == "__main__":
    exp = RdpsoBaseExperiment(wf_name="Epigenomics_24",
                              W=0.2, C1=0.5, C2=0.5,
                              GEN=300, N=20)
    result = repeat(exp, 60)
    print(result)
    # result = exp()
    sts = interval_statistics(result)
    print("Statistics: {0}".format(interval_stat_string(sts)))
    print("Average: {0}".format(numpy.mean(result)))
    pass

