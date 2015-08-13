from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.Utility import wf, Utility, wf_set
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.core.vizualization.Vizualization import Vizualizator

rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=20, transfer_time=100)
estimator = ExperimentEstimator(ideal_flops=20, transfer_nodes=100, transfer_blades=100)

def do_exp(wf_name):
    _wf = wf_set(wf_name)
    heft_schedule = run_heft(_wf, rm, estimator)
    Utility.validate_static_schedule(_wf, heft_schedule)
    makespan = Utility.makespan(heft_schedule)
    # Vizualizator.create_jedule_visualization(heft_schedule, "D:\Projects\\new_heft\\heft\\heft\\vizual", "heft")
    return makespan
    # print("Heft makespan: {0}".format(Utility.makespan(heft_schedule)))

if __name__ == "__main__":
    # result = do_exp(["Montage_25", 2000, "Montage_50", 5000])
    result = do_exp(["Montage_25", 2000, "CyberShake_30", 3000, "Inspiral_30", 1000])
    print(result)
