from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.peft.DSimplePeft import run_peft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

#Mishanya
from heft.algs.heft.simple_heft import StaticHeftPlanner


#rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
#estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
#                                    ideal_flops=20, transfer_time=100)
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=20, transfer_time=100)

def do_exp_HEFT(wf_name, trans):
    _wf = wf("" + wf_name)

    estimator.transfer_time = trans
    #estim = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
    #                                ideal_flops=20, transfer_time=trans)

    heft_schedule = run_heft(_wf, rm, estimator)
    #return heft_schedule
    Utility.validate_static_schedule(_wf, heft_schedule)

    makespan = Utility.makespan(heft_schedule)
    return makespan
    # print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

#Mishanya
def do_exp_PEFT(wf_name, trans):
    _wf = wf(wf_name)
    estimator.transfer_time = trans
    #estim = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
    #                                ideal_flops=20, transfer_time=trans)

    peft_schedule = run_peft(_wf, rm, estimator)
    #return peft_schedule
    Utility.validate_static_schedule(_wf, peft_schedule)

    makespan = Utility.makespan(peft_schedule)
    return makespan





if __name__ == "__main__":
    repeat_count = 1
    #wf_list = ["Montage_25"]
    wf_list = ["Montage_25", "Montage_30", "Montage_50", "Montage_100", "Montage_250",
               "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
               "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
               "Inspiral_30", "Inspiral_50", "Inspiral_100", "Sipht_30", "Sipht_60", "Sipht_100"]
    #wf_list = ["Montage_25", "Montage_50", "Montage_100", "Montage_250", "CyberShake_30",
    #           "CyberShake_50", "CyberShake_100", "Inspiral_30", "Inspiral_100", "Sipht_30", "Sipht_60", "Sipht_100"]
    trans_list = [10, 100, 500, 1000, 10000]
    for wf_cur in wf_list:
        print(wf_cur)

        print("    HEFT")
        heft_result = [do_exp_HEFT(wf_cur, 100) for _ in range(repeat_count)]
    #print(min(result))
        print("    " + str(heft_result))

        print("    PEFT")
        peft_result = [do_exp_PEFT(wf_cur, 100) for _ in range(repeat_count)]
        #print("finish")
    #print(max(result))
        print("    " + str(peft_result))
        #for _ in range(repeat_count):
        #profit_list = [(round((((do_exp_HEFT(wf_cur, trans) / do_exp_PEFT(wf_cur, trans)) - 1) * 100), 2), trans) for trans in [x * 10 for x in range(101)]]
        #profit_list = [(round((((do_exp_HEFT(wf_cur, trans) / do_exp_PEFT(wf_cur, trans)) - 1) * 100), 2), trans) for trans in [x for x in trans_list]]
        #profit_list = [(round((((do_exp_PEFT(wf_cur, trans) / do_exp_HEFT(wf_cur, trans)) - 1) * 100), 2), trans) for trans in [x * 10 for x in range(11)]]
        #profit_list = [(round((((do_exp_PEFT(wf_cur, trans) / do_exp_HEFT(wf_cur, trans)) - 1) * 100), 2), trans) for trans in [x for x in trans_list]]
        profit_list = [(round((((heft_result[0] / peft_result[0]) - 1) * 100), 2), trans) for trans in [100]]
        print("    " + str(profit_list))
        #file = open("F:\eScience\Work\experiments\HEFTvsPEFT\\" + wf_cur + ".txt", 'w')
        #for (profit, trans) in profit_list:
        #    file.write(str(profit) + "    " + str(trans) + "\n")


