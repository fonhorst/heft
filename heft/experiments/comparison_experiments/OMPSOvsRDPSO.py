from copy import deepcopy
import random
from deap.base import Toolbox
import numpy
#----
#from heft.algs.pso.rdpso.ordering_operators import build_schedule, generate, ordering_update, fitness
import heft.algs.pso.ordering_operators as om_order
import  heft.algs.pso.rdpso.ordering_operators as rd_order
#from heft.algs.pso.rdpso.rdpso import run_pso, initMapMatrix, initRankList
import heft.algs.pso.sdpso as om
import heft.algs.pso.rdpso.rdpso as rd
#from heft.algs.pso.rdpso.mapping_operators import update as mapping_update
import heft.algs.pso.mapping_operators as om_map
import heft.algs.pso.rdpso.mapping_operators as rd_map
#---
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
        heft_schedule = self.heft_schedule()

        wf_dag = HeftHelper.convert_to_parent_children_map(_wf)
        jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
        nodes = rm.get_nodes()
        mapMatrix = rd.initMapMatrix(jobs, nodes, estimator)
        rankList = rd.initRankList(wf_dag, nodes, estimator)
        ordFilter = rd.filterList(_wf)

        toolbox = self.toolbox(mapMatrix, rankList, ordFilter)

        pop, log, best = rd.run_pso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, initial_pop=None,
            w=self.W, c1=self.C1, c2=self.C2, n=self.N, rm=rm, wf=_wf, estimator=estimator, mapMatrix=mapMatrix, rankList=rankList, ordFilter=ordFilter,
        )

        schedule = rd_order.build_schedule(_wf, rm, estimator,  best, mapMatrix, rankList, ordFilter)

        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        #print("Final makespan: {0}".format(makespan))
        #print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
        return makespan

    def toolbox(self, mapMatrix, rankList, ordFilter):

        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()



        heft_particle = rd_order.generate(_wf, rm, estimator, mapMatrix, rankList, ordFilter, heft_schedule)

        heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.00 else rd_order.generate(_wf, rm, estimator, mapMatrix, rankList, ordFilter) for _ in range(n)]

        #def componoud_update(w, c1, c2, p, best, pop, g):
        def componoud_update(w, c1, c2, p, best, pop):
            #if g%2 == 0:
            rd_map.update(w, c1, c2, p.mapping, best.mapping, pop)
            #else:
            rd_order.ordering_update(w, c1, c2, p.ordering, best.ordering, pop)

        toolbox = Toolbox()
        toolbox.register("population", heft_gen)
        toolbox.register("fitness", rd_order.fitness, _wf, rm, estimator)
        toolbox.register("update", componoud_update)
        return toolbox


    pass

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

        pop, log, best = om.run_pso(
            toolbox=toolbox,
            logbook=logbook,
            stats=stats,
            gen_curr=0, gen_step=self.GEN, invalidate_fitness=True, initial_pop=None,
            w=self.W, c1=self.C1, c2=self.C2, n=self.N,
        )

        schedule = om_order.build_schedule(_wf, rm, estimator,  best)

        Utility.validate_static_schedule(_wf, schedule)
        makespan = Utility.makespan(schedule)
        #print("Final makespan: {0}".format(makespan))
        #print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))
        return makespan

    def toolbox(self):

        _wf, rm, estimator = self.env()
        heft_schedule = self.heft_schedule()

        heft_particle = om_order.generate(_wf, rm, estimator, heft_schedule)

        heft_gen = lambda n: [deepcopy(heft_particle) if random.random() > 1.00 else om_order.generate(_wf, rm, estimator) for _ in range(n)]

        def componoud_update(w, c1, c2, p, best, pop, min=-1, max=1):
            om_map.update(w, c1, c2, p.mapping, best.mapping, pop)
            om_order.ordering_update(w, c1, c2, p.ordering, best.ordering, pop, min=min, max=max)

        toolbox = Toolbox()
        toolbox.register("population", heft_gen)
        toolbox.register("fitness", om_order.fitness, _wf, rm, estimator)
        toolbox.register("update", componoud_update)
        return toolbox


    pass




if __name__ == "__main__":
    wf_list = ["Inspiral_30"]
    #wf_list = ["Montage_25", "Montage_50",# "Montage_100", #"Montage_250",
                   #"Epigenomics_24", "Epigenomics_46",# "Epigenomics_72", "Epigenomics_100",
                   #"CyberShake_30", "CyberShake_50",# "CyberShake_75", "CyberShake_100",
                   #"Inspiral_30", "Inspiral_50", "Sipht_30", "Sipht_60"]


        #for (profit, trans) in profit_list:
        #    file.write(str(profit) + "    " + str(trans) + "\n")   `
    w = 0.1
    c1 = 0.6
    c2 = 0.2
    gen = 300
    n = 20
    fileRes = open("F:\eScience\Work\experiments\OMPSOvsRDPSO\Sipht_30 c1_06 c2_02 gen_300 n_20 FILTER only RD TEST.txt", 'w')
    fileInfo = open("F:\eScience\Work\experiments\OMPSOvsRDPSO\Sipht_30 c1_06 c2_02 gen_300 n_20 FILTER info only RD TEST.txt", 'w')

    exp_om = OmpsoBaseExperiment(wf_name=wf_list[0],
                              W=w, C1=c1, C2=c2,
                              GEN=gen, N=n)
    result2 = repeat(exp_om, 100)
    sts2 = interval_statistics(result2)
    #for w in [x / 10 for x in range(15)]:
    for _ in [1]:
        print(w)
        wf_cur = wf_list[0]
        print(wf_cur)
        exp_rd = RdpsoBaseExperiment(wf_name=wf_cur,
                              W=0.1, C1=0.6, C2=0.2,
                              GEN=gen, N=n)

        result1 = repeat(exp_rd, 100)
        sts1 = interval_statistics(result1)


        fileInfo.write("OM " + str(w) + " " + str(sts2) + "\n")
        fileInfo.write("RD " + str(w) + " " + str(sts1) + "\n")
        fileInfo.write("\n")
        profit = (round((((sts2[0] / sts1[0]) - 1) * 100), 2))
        fileRes.write(str(w) + "    " + str(profit) + "\n")
        print("profit = " + str(profit))
        print("    RD    " + str(sts1))
        print("    OM    " + str(sts2))
    #res_list[iter] = (round((((result2[0] / result1[0]) - 1) * 100), 2))
    # result = exp()
    #sts = interval_statistics(result)
    #print("Statistics: {0}".format(interval_stat_string(sts)))
    #print("Average: {0}".format(numpy.mean(result)))



    pass




