from copy import deepcopy

from heft.algs.ga.GAImplementation.GAFunctions2 import mark_finished
from heft.algs.ga.GAImplementation.GAImpl import GAFactory
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.BladeExperimentalManager import TransferCalcExperimentEstimator, ExperimentEstimator
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.ResourceManager import Schedule
from heft.core.environment.Utility import wf, Utility
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg


"""
:return Schedule
"""
def do_run_heft():
    heft_schedule = run_heft(_wf, rm, estimator)
    #Utility.validate_static_schedule(_wf, heft_schedule)
    print("HEFT makespan: " + str(Utility.makespan(heft_schedule)))
    return heft_schedule


def do_run_ga(initial_schedule):
    def default_fixed_schedule_part(resource_manager):
         fix_schedule_part = Schedule({node: [] for node in HeftHelper.to_nodes(resource_manager.get_resources())})
         return fix_schedule_part

    fix_schedule_part = default_fixed_schedule_part(rm)
    ((the_best_individual, pop, schedule, iter_stopped), logbook) = GAFactory.default()\
        .create_ga(silent=False,
                   wf=_wf,
                   resource_manager=rm,
                   estimator=estimator,
                   ga_params=GA_PARAMS)(fix_schedule_part, initial_schedule)

    _validate(wf, estimator, schedule)
    print("GA makespan: " + str(Utility.makespan(schedule)))
    return schedule


def _validate(wf, estimator, schedule):
    ax_makespan = Utility.makespan(schedule)
    seq_time_validaty = Utility.validateNodesSeq(schedule)
    sched = deepcopy(schedule)
    mark_finished(sched)
    Utility.validate_static_schedule(_wf, schedule)
    ## TODO: obsolete remove it later
    #  dependency_validaty = Utility.validateParentsAndChildren(sched, wf)
    #  transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(sched, wf, estimator)
    #  print("=============Results====================")
    #  print("              Makespan %s" % str(max_makespan))
    #  print("          Seq validaty %s" % str(seq_time_validaty))
    #  print("   Dependancy validaty %s" % str(dependency_validaty))
    #  print("    Transfer validaty %s" % str(transfer_dependency_validaty)


if __name__ == '__main__':

    ideal_flops = 1

    _wf = wf("Montage_25")
    # rm = ExperimentResourceManager(rg.r([0.5, 0.75, 1.25, 1.5]))
    rm = ExperimentResourceManager(rg.r([1.2, 1.2, 1.2, 1.0, 1.0, 1.0, 0.8, 0.8, 0.8]))
    # estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
    #                                     ideal_flops=ideal_flops, transfer_time=100)
    estimator = ExperimentEstimator(ideal_flops=ideal_flops, transfer_nodes=100, transfer_blades=100)

    ## transfer_nodes means now channel bandwidth
    # MB_100_CHANNEL = 100*1024*1024
    # MB_100_CHANNEL = 7*1024*1024
    # estimator = TransferCalcExperimentEstimator(ideal_flops=ideal_flops, transfer_nodes=MB_100_CHANNEL, transfer_blades=100)

    GA_PARAMS = {
            "Kbest": 5,
            "population": 100,
            "crossover_probability": 0.4, #0.3
            "replacing_mutation_probability": 0.2, #0.1
            "sweep_mutation_probability": 0.3, #0.3
            "generations": 25
    }

    heft_schedule = do_run_heft()
    ga_schedule = do_run_ga(initial_schedule=heft_schedule)