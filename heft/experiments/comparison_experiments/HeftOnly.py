from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator, \
    TransferCalcExperimentEstimator
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

ideal_flops = 1

rm = ExperimentResourceManager(rg.r([0.5, 0.75, 1.25, 1.5]))
# estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
#                                     ideal_flops=ideal_flops, transfer_time=100)
# estimator = ExperimentEstimator(ideal_flops=ideal_flops, transfer_nodes=100, transfer_blades=100)

## transfer_nodes means now channel bandwidth
MB_100_CHANNEL = 100*1024*1024
estimator = TransferCalcExperimentEstimator(ideal_flops=ideal_flops, transfer_nodes=MB_100_CHANNEL, transfer_blades=100)

def do_exp(wf_name):
    _wf = wf(wf_name)
    heft_schedule = run_heft(_wf, rm, estimator)
    Utility.validate_static_schedule(_wf, heft_schedule)
    makespan = Utility.makespan(heft_schedule)
    return makespan
    # print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

if __name__ == "__main__":
<<<<<<< HEAD
    result = do_exp("Inspiral_30")
=======
    result = do_exp("Montage_25")
>>>>>>> 35e937ac86337ec565b9d4f0fd5dab953e8d2e67
    print(result)
