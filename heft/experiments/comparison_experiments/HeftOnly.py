from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=20, transfer_time=100)

def do_exp(wf_name):
    _wf = wf(wf_name)
    heft_schedule = run_heft(_wf, rm, estimator)
    Utility.validate_static_schedule(_wf, heft_schedule)
    makespan = Utility.makespan(heft_schedule)
    return makespan
    # print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

if __name__ == "__main__":
    repeat_count = 1
    result = [do_exp("Montage_50") for _ in range(repeat_count)]
    print(result)
