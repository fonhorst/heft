from heft.algs.heft.DeadlineHeft import run_heft
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.Utility import wf, Utility, wf_set
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.core.environment.BaseElements import Workflow
from heft.core.environment.BaseElements import Task

rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=20, transfer_time=100)

def do_exp(wf_info):
    _work_wf = wf_set(wf_info)

    heft_schedule = run_heft(_work_wf, rm, estimator)
    Utility.validate_static_schedule(_work_wf, heft_schedule)
    makespan = Utility.makespan(heft_schedule)
    return makespan

if __name__ == "__main__":
    result = do_exp(["Sipht_30", 5000, "Montage_25", 2000, "Inspiral_30", 0])
    print(result)
