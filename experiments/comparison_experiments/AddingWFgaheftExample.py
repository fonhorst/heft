## TODO: it's an example. It should be rewritten and moved to unit tests directory
## GaHeftvsHeft with wf adding
from algs.ga.GAImplementation.GAFunctions2 import mark_finished

from algs.heft.DSimpleHeft import DynamicHeft
## reliability doesn't matter anything here
from core.environment import Utility
from experiments.comparison_experiments.executors.GaHeftExecutor import GAComputationManager
from core.environment.ResourceManager import Schedule
from experiments.comparison_experiments.common import ExecutorRunner

wf_added_times = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
#wf_added_times = [0.1]

initial_wf_name = "Montage_30"
added_wf_name = "Montage_25"

initial_wf = ExecutorRunner.get_wf(initial_wf_name, "00")
added_wf = ExecutorRunner.get_wf(added_wf_name, "10")
bundle = Utility.get_default_bundle()
(estimator, resource_manager, initial_schedule) = ExecutorRunner.get_infrastructure(bundle, 1.0, False)

## planning for initial wf
heft = DynamicHeft(initial_wf, resource_manager, estimator)
empty_schedule = Schedule({node:[] for node in heft.get_nodes()})
ga = GAComputationManager(15,
                          initial_wf,
                          resource_manager,
                          estimator)

ga_initial_schedule = ga._get_ga_alg()(empty_schedule, None)[2]

all_initial_wf_time = Utility.makespan(ga_initial_schedule)

print("Initial time: " + str(all_initial_wf_time))

n = 5
## planning for added wf
def gaheft_reschedule(wf_added_time):

    copy_gaheft_schedule = Schedule({node:[item for item in items] for (node, items) in ga_initial_schedule.mapping.items()})

    added_time = all_initial_wf_time * wf_added_time

    mark_finished(copy_gaheft_schedule)
    gaheft_added = DynamicHeft(added_wf, resource_manager, estimator)
    gaheft_added.current_time = added_time
    gaheft_added_schedule = gaheft_added.run(copy_gaheft_schedule)
    new_ga = GAComputationManager(15,
                          added_wf,
                          resource_manager,
                          estimator)

    gaheft_added_schedule = new_ga.run(gaheft_added_schedule, added_time, False)[2]

    mark_finished(gaheft_added_schedule)

    nodes_seq_validaty = Utility.validateNodesSeq(gaheft_added_schedule)
    if nodes_seq_validaty is not True:
        raise Exception("Check for nodes_seq_validaty didn't pass")
    initial_wf_validaty = Utility.validateParentsAndChildren(gaheft_added_schedule, initial_wf)
    if initial_wf_validaty is not True:
        raise Exception("Check for initial_wf_validaty didn't pass")
    added_wf_validaty = Utility.validateParentsAndChildren(gaheft_added_schedule, added_wf)
    if added_wf_validaty is not True:
        raise Exception("Check for added_wf_validaty didn't pass")
    #print("All Ok!")
    result = Utility.makespan(gaheft_added_schedule)
    return result

result = [[gaheft_reschedule(wf_added_time)for i in range(n)] for wf_added_time in wf_added_times]
print(str(result))
