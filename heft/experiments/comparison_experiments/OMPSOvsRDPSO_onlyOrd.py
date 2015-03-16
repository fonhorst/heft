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
from heft.experiments.pso.ompso_base_experiment import OmpsoBaseExperiment
from heft.experiments.pso.rdpso_base_experiment_ordering import RdpsoBaseExperiment
from heft.experiments.comparison_experiments.HeftOnly import do_exp
from heft.core.environment.Utility import wf
import heft.experiments.pso.rdpso_base_experiment
from heft.experiments.common import AbstractExperiment
from heft.algs.heft.HeftHelper import HeftHelper
import math

def get_data_rate(jobslist):
            jobs_copy = jobslist.copy()
            total_job_rate = 0
            total_runtime = 0
            total_datasize = 0
            for it in range(len(jobs_copy)):
                job = jobs_copy.pop()
                cur_datasize = 0
                for file in job.input_files.items():
                    cur_datasize = cur_datasize + file[1].size
                total_job_rate = total_job_rate + (cur_datasize / job.runtime)
                total_runtime = total_runtime + job.runtime
                total_datasize = total_datasize + cur_datasize
            total_job_rate = total_job_rate / len(jobslist)
            total_runtime = total_runtime / len(jobslist)
            total_datasize = total_datasize / len(jobslist)

            #return total_datasize / total_runtime
            return total_job_rate

if __name__ == "__main__":
    wf_list = ["Montage_100"]

    #wf_list = ["Montage_25", "Montage_50",# "Montage_100", #"Montage_250",
    #               "Epigenomics_24",# "Epigenomics_46",# "Epigenomics_72", "Epigenomics_100",
    #               "CyberShake_30",# "CyberShake_50",# "CyberShake_75", "CyberShake_100",
    #               "Inspiral_30",# "Inspiral_50",
    #               "Sipht_30"]#, "Sipht_60"]

        #for (profit, trans) in profit_list:
        #    file.write(str(profit) + "    " + str(trans) + "\n")   `
    w = 0.2
    c1 = 0.6
    c2 = 0.2
    gen = 5000
    n = 100
    execCount = 12
    #fileRes = open("C:\Melnik\Experiments\Work\PSO_compare\clean 300 20 (0.1 0.6 0.2).txt", 'w')
    #fileInfo = open("C:\Melnik\Experiments\Work\PSO_compare\ data_intensive_average 0.2 0.6 0.2 500 50 info.txt", 'w')
    #fileInfo.write(str(w) + " " + str(c1) + " " + str(c2) + " " + str(gen) + " " + str(n) + "\n")
    #fileRes.write("# heftProfit     profit" + "\n")
    for wf_cur in wf_list:
            print(wf_cur)
            #wf_dag = HeftHelper.convert_to_parent_children_map(wf(wf_cur))
            #jobs = set(wf_dag.keys()) | set(x for xx in wf_dag.values() for x in xx)
            #data_intensive = 3100000 / get_data_rate(jobs) * 100
            #data_intensive = 100
            #print(data_intensive)

        #curFileOM = open("C:\Melnik\Experiments\Work\PSO_compare\\result\\" + wf_cur + "\\" +  "OM w_0.2 c1_0.6 c2_test gen_300 n_20.txt", 'w')
        #curFileRD = open("C:\Melnik\Experiments\Work\PSO_compare\\result\\" + wf_cur + "\\" +  "RD w_0.2 c1_0.6 c2_test gen_300 n_20.txt", 'w')
        #curFileRD2 = open("C:\Melnik\Experiments\Work\PSO_compare\\result\\" + wf_cur + "\\" +  "RD2 w_0.2 c1_0.6 c2_test gen_300 n_20.txt", 'w')
        #curFile.write("#C1    OM    RD    RDwithMap    Heft" + "\n")
        #for wcur in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5
            #c2 = wcur
            #print(c2)

            exp_om = OmpsoBaseExperiment(wf_name=wf_cur,
                                  W=w, C1=c1, C2=c2,
                                  GEN=gen, N=n, data_intensive=100)
            result2 = repeat(exp_om, execCount)
            sts2 = interval_statistics([makespan for (makespan, pop) in result2])

            #print("    om finish")


            exp_rd = RdpsoBaseExperiment(wf_name=wf_cur,
                              W=w, C1=c1, C2=c2,
                              GEN=gen, N=n, data_intensive=100)
            result1 = repeat(exp_rd, execCount)
            sts1 = interval_statistics([makespan for (makespan, pop) in result1])

            #print("    rd finish")
            """
            exp_rdM = heft.experiments.pso.rdpso_base_experiment.RdpsoBaseExperiment(wf_name=wf_cur,
                              W=w, C1=c1, C2=c2,
                              GEN=gen, N=n)
            result3 = repeat(exp_rdM, execCount)
            sts3 = interval_statistics(result3)

            print("    rd2 finish")
            """
            heftRes = do_exp(wf_cur, 100)

            #fileInfo.write(wf_cur + "\n")
            #fileInfo.write("HEFT " + str(heftRes) + "\n")
            #fileInfo.write("OM " + str(sts2) + "\n")
            #fileInfo.write("RD " + str(sts1) + "\n")
            #fileInfo.write("RDwithMap " + str(sts3) + "\n")
            #fileInfo.write("\n")
            #profit = (round((((sts2[0] / sts1[0]) - 1) * 100), 2))
            #profit2 = (round((((sts2[0] / sts3[0]) - 1) * 100), 2))
            #heftProfit = (round((((heftRes / sts1[0]) - 1) * 100), 2))
        #fileRes.write("# " + wf_cur + "\n")
        #fileRes.write(str(profit) + "    " + str(heftProfit) + "\n")

        #curFile.write("#    OM        RD" + "\n")
            #for it in range(execCount):
            #    curFile.write(str(round(result2[it], 2)) + "    " + str(round(result1[it],2)) + "\n")

            #curFileOM.write(str(c2) + "    " + str(sts2[0]) + "    " + str(sts2[1]) + "    " + str(sts2[2]) + "    " + str(heftRes) + "\n")
            #curFileRD.write(str(c2) + "    " + str(sts1[0]) + "    " + str(sts1[1]) + "    " + str(sts1[2]) + "    " + str(heftRes) + "\n")
            #curFileRD2.write(str(c2) + "    " + str(sts3[0]) + "    " + str(sts3[1]) + "    " + str(sts3[2]) + "    " + str(heftRes) + "\n")



            #print("profit = " + str(profit))
            #print("profitM = " + str(profit2))
            #print("heftProfit = " + str(heftProfit))
            print("    HEFT  " + str(heftRes))
            print("    OM    " + str(sts2))
            print("    RD    " + str(sts1))
            #print("    RD2    " + str(sts3))

        #curFileOM.close()
        #curFileRD.close()
        #curFileRD2.close()
    #res_list[iter] = (round((((result2[0] / result1[0]) - 1) * 100), 2))
    # result = exp()
    #sts = interval_statistics(result)
    #print("Statistics: {0}".format(interval_stat_string(sts)))
    #print("Average: {0}".format(numpy.mean(result)))


    print("\n" + "     FINISH!!!!")
    pass






