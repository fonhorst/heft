from heft.experiments.aggregate_utilities import interval_statistics, interval_stat_string
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.pso.ompso_base_experiment import OmpsoBaseExperiment
from heft.experiments.pso.rdpso_base_experiment_ordering import RdpsoBaseExperiment
from heft.experiments.comparison_experiments.HeftOnly import do_exp
from heft.core.environment.Utility import wf
from heft.algs.heft.HeftHelper import HeftHelper

if __name__ == "__main__":
    wf_cur = "Montage_25"

    w = 0.2
    c1 = 0.6
    c2 = 0.2
    gen = 10
    n = 4
    execCount = 1
    data_intensive = 100

    exp_om = OmpsoBaseExperiment(wf_name=wf_cur,
    #exp_rd = RdpsoBaseExperiment(wf_name=wf_cur,
                                  W=w, C1=c1, C2=c2,
                                  GEN=gen, N=n, data_intensive=data_intensive)

    #result = repeat(exp_om, execCount)
    result = repeat(exp_om, execCount)

    #file = open("C:\Melnik\Experiments\Work\PSO_compare\populations\OM cyber.txt", 'w')
    #file.write("#w = " + str(w) + " c1 = " + str(c1) + " c2 = " + str(c2) + "\n")
    #file.write("#gen    result" + "\n")

    res_list = [0 for _ in range(gen)]
    for i in range(execCount):
        cur_list = result[i][1]
        print(str(cur_list))
        for j in range(gen):
            res_list[j] = res_list[j] + cur_list[j]

    res_list = [x / execCount for x in res_list]
    print("res_list = " + str(res_list))
    #for i in range(gen):
        #file.write(str(i) + "   " + str(res_list[i]) + "\n")
    #sts = interval_statistics(result[0])
    #heftRes = do_exp(wf_cur, data_intensive)

    #print("res_list = " + str(res_list))

    #print("    HEFT  " + str(heftRes))
    #print("    OM    " + str(sts))

    print("finish")