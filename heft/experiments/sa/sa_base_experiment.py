from pstats import Stats
from deap import tools
from deap.base import Toolbox
import numpy

from heft.algs.heft.DSimpleHeft import run_heft
from heft.algs.heft.HeftHelper import HeftHelper
from heft.algs.pso.sdpso import schedule_to_position, generate
from heft.algs.sa.SimulatedAnnealingScheme import run_sa
from heft.algs.sa.mappingops import energy, update_T, mapping_neighbor, transition_probability, State
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import Utility, wf
from heft.algs.common.mapordschedule import build_schedule, MAPPING_SPECIE, ORDERING_SPECIE
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg

## TODO: need to test all of it
from heft.experiments.cga.utilities.common import repeat

_wf = wf("Montage_25")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                            ideal_flops=20, transfer_time=100)
sorted_tasks = HeftHelper.heft_rank(_wf, rm, estimator)

heft_schedule = run_heft(_wf, rm, estimator)
heft_mapping = schedule_to_position(heft_schedule).entity



initial_state = State()
initial_state.mapping = heft_mapping
# initial_state.mapping = generate(_wf, rm, estimator, 1)[0].entity
initial_state.ordering = sorted_tasks

T, N = 20, 1000



toolbox = Toolbox()
toolbox.register("energy", energy, _wf, rm, estimator)
toolbox.register("update_T", update_T, T)
toolbox.register("neighbor", mapping_neighbor, _wf, rm, estimator, 1)
toolbox.register("transition_probability", transition_probability)
# use just a const to define number of attempts
toolbox.register("attempts_count", lambda T: 100)

logbook = tools.Logbook()
logbook.header = ["gen", "T", "val"]

stats = tools.Statistics(lambda ind: ind.energy.values[0])
stats.register("val", lambda arr: arr[0])

def do_exp():
    best, log, current = run_sa(
        toolbox=toolbox,
        logbook=logbook,
        stats=stats,
        initial_solution=initial_state, T=T, N=N
    )

    solution = {MAPPING_SPECIE: [item for item in best.mapping.items()], ORDERING_SPECIE: best.ordering}
    schedule = build_schedule(_wf, estimator, rm, solution)
    Utility.validate_static_schedule(_wf, schedule)
    makespan = Utility.makespan(schedule)
    heft_makespan = Utility.makespan(heft_schedule)
    print("Final makespan: {0}".format(makespan))
    print("Heft makespan: {0}".format(heft_makespan))
    return makespan

if __name__ == "__main__":
    result = repeat(do_exp, 10)
    print(result)
    print("Mean: {0}".format(numpy.mean(result)))
    pass

