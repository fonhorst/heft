from GA.DEAPGA.GAImplementation.NewSchedulerBuilder import NewScheduleBuilder
from GA.DEAPGA.GAImplementation.ScheduleBuilder import ScheduleBuilder
from environment.Utility import Utility

def temp_fitness(triplet):
    (chromo, builder, current_time) = triplet
    schedule = builder(chromo, current_time)
    time = Utility.makespan(schedule)
    return (1/time,)
    # return (1,)

def create_builder(ga_functions, fixed_schedule_part):
        # builder = ScheduleBuilder(ga_functions.workflow, ga_functions.resource_manager, ga_functions.estimator, ga_functions.task_map, ga_functions.node_map, fixed_schedule_part)
        builder = NewScheduleBuilder(ga_functions.workflow, ga_functions.resource_manager, ga_functions.estimator, ga_functions.task_map, ga_functions.node_map, fixed_schedule_part)
        ## TODO: redesign it later
        return builder

def temp_fit_for_all(elements):
    elts = list(elements)
    return [temp_fitness(el) for el in elts]
    # size = len(elements)
    # return [(1,) for i in range(size)]