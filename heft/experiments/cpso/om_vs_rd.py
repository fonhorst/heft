from heft.experiments.cpso.compso import CompsoBaseExperiment
from heft.experiments.cpso.crdpso import CrdpsoBaseExperiment
import numpy
from heft.experiments.aggregate_utilities import interval_statistics, interval_stat_string
from heft.experiments.cga.utilities.common import repeat
from heft.experiments.comparison_experiments.HeftOnly import do_exp

if __name__ == "__main__":

    wf = "Montage_25"

    heftRes = do_exp(wf)
    print("HEFT: " + str(heftRes))

    exp1 = CrdpsoBaseExperiment(wf_name=wf,
                                  W=0.5, C1=1.6, C2=1.2,
                                  GEN=10, N=20, MAX_FLOPS=30, SUM_FLOPS=80)
    result1 = repeat(exp1, 2)
    print(result1)
    sts1 = interval_statistics(result1)
    print("Statistics: {0}".format(interval_stat_string(sts1)))
    print("Average: {0}".format(numpy.mean(result1)))

    exp2 = CompsoBaseExperiment(wf_name=wf,
                                  W=0.5, C1=1.6, C2=1.2,
                                  GEN=10, N=20, MAX_FLOPS=30, SUM_FLOPS=80)
    result2 = repeat(exp2, 2)
    print(result2)
    sts2 = interval_statistics(result2)
    print("Statistics: {0}".format(interval_stat_string(sts2)))
    print("Average: {0}".format(numpy.mean(result2)))

