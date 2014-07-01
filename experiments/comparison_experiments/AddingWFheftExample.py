## TODO: it's an example. It should be rewritten and moved to unit tests directory
## GaHeftvsHeft with wf adding
from algs.ga.GAImplementation.GAFunctions2 import mark_finished

from algs.heft.DSimpleHeft import DynamicHeft
## reliability doesn't matter anything here
from core.environment import Utility
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
empty_schedule  = Schedule({node:[] for node in heft.get_nodes()})
heft_schedule = heft.run(empty_schedule)

all_initial_wf_time = Utility.makespan(heft_schedule)
print("Initial time: " + str(all_initial_wf_time))

n = 1

## planning for added wf
def heft_reschedule(wf_added_time):

    copy_heft_schedule = Schedule({node:[item for item in items] for (node, items) in heft_schedule.mapping.items()})

    added_time = all_initial_wf_time * wf_added_time
    heft_added = DynamicHeft(added_wf, resource_manager, estimator)
    heft_added.current_time = added_time
    heft_added_schedule = heft_added.run(copy_heft_schedule)

    mark_finished(heft_added_schedule)

    nodes_seq_validaty = Utility.validateNodesSeq(heft_added_schedule)
    if nodes_seq_validaty is not True:
        raise Exception("Check for nodes_seq_validaty didn't pass")
    initial_wf_validaty =  Utility.validateParentsAndChildren(heft_added_schedule, initial_wf)
    if initial_wf_validaty is not True:
        raise Exception("Check for initial_wf_validaty didn't pass")
    added_wf_validaty =  Utility.validateParentsAndChildren(heft_added_schedule, added_wf)
    if added_wf_validaty is not True:
        raise Exception("Check for added_wf_validaty didn't pass")
    #print("All Ok!")
    result = Utility.makespan(heft_added_schedule)
    return result

result = [[heft_reschedule(wf_added_time) for i in range(n)] for wf_added_time in wf_added_times]
print(str(result))








#class NewWFAdded(BaseEvent):
#    def __init__(self, new_wf):
#        self.new_wf = new_wf
#
#    def __str__(self):
#        return "NewWFAdded"

#class ExtHeftExecutor(HeftExecutor):
#    def __init__(self, heft_planner_factory, base_fail_duration, base_fail_dispersion ,
#                 initial_schedule = None, logger=None, time_koeff=0.1, new_wf=None):
#        heft_planner = heft_planner_factory()
#        super().__init__(heft_planner, base_fail_dispersion, base_fail_dispersion, initial_schedule, logger)
#
#        self.heft_planner_factory = heft_planner_factory
#        self.time_koeff = time_koeff
#        self.new_wf = new_wf
#
#    def init(self):
#        super().init()
#        if self.new_wf is not None:
#            wf_event = NewWFAdded(self.new_wf)
#            self.post(wf_event)
#
#    def _check_fail(self, task, node):
#            return False
#
#
#    def event_arrived(self, event):
#        if isinstance(event, TaskStart):
#            super().event_arrived(event)
#            return
#        if isinstance(event, TaskFinished):
#            super().event_arrived(event)
#            return
#        if isinstance(event, NewWFAdded):
#            self._new_wf_added(event)
#            return
#        if isinstance(event, NodeFailed):
#            raise Exception("Invalid event: " + str(event))
#        if isinstance(event, NodeUp):
#            raise Exception("Invalid event: " + str(event))
#
#        raise Exception("Unknown event: " + str(event))
#
#    def _new_wf_added(self, event):
#        new_wf = event.new_wf
#        heft_planner = self.heft_planner_factory(new_wf)
#        new_schedule = self.heft_planner.run(self.current_schedule)
#        post_new_events(new_schedule - self.current_schedule)
#        self.current_schedule = new_schedule
#        pass
#
#    def _reschedule(self,event):
#        raise Exception("Invalid call. This function must not be called in this executor. Perhaps, there is a misttake in logic.")