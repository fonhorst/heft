from deap import tools
from deap.base import Toolbox

from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.sdpso import schedule_to_position
from heft.algs.sa.SimulatedAnnealingScheme import run_sa
from heft.algs.sa.mappingops import energy, update_T, mapping_neighbor, transition_probability, State
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

## TODO: need to test all of it

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)
heft_mapping = schedule_to_position(heft_schedule).entity.mapping

initial_state = State()
initial_state.mapping = heft_mapping
initial_state.ordering = sorted_tasks

T, N = 1000, 50



toolbox = Toolbox()
toolbox.register("energy", energy, _wf, rm, estimator)
toolbox.register("update_T", update_T)
toolbox.register("mapping_neighbor", mapping_neighbor, _wf, rm, estimator)
toolbox.register("transition_probability", transition_probability)




logbook = tools.Logbook()
logbook.header = ["gen", "T"]

def do_exp():
    best, log, current = run_sa(
        toolbox=toolbox,
        logbook=logbook,
        stats=None,
        initial_solution=initial_state, T=T, N=N
    )

    solution =  {MAPPING_SPECIE: best.mapping, ORDERING_SPECIE: best.ordering}
    schedule = build_schedule(_wf, estimator, rm, solution)
    makespan = Utility.makespan(schedule)
    print("Final makespan: {0}".format(makespan))
    pass

if __name__ == "__main__":
    do_exp()
    pass

