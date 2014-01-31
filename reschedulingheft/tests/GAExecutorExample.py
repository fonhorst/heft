import math
from environment.EventAutomata import EventAutomata
from environment.Utility import Utility
from reschedulingheft.DSimpleHeft import DynamicHeft
from reschedulingheft.GAExecutor import GAExecutor
from reschedulingheft.HeftExecutor import HeftExecutor
from reschedulingheft.HeftHelper import HeftHelper
from reschedulingheft.concrete_realization import ExperimentEstimator, ExperimentResourceManager

def main(is_silent, wf_name, bundle):
    ## 0. create reliability

    ##======================
    ## Load generated by GA schedule and resource config
    ##======================
    ##wf_name = "CyberShake_30"
    ##wf_name = "CyberShake_50"

    dax1 = '..\\..\\resources\\' + wf_name + '.xml'
    # dax2 = '..\\..\\resources\\' + 'CyberShake_30' + '.xml'
    ##dax1 = '..\\..\\resources\\Montage_50.xml'
    wf_start_id_1 = "00"
    task_postfix_id_1 = "00"
    deadline_1 = 1000

    wf = Utility.readWorkflow(dax1, wf_start_id_1, task_postfix_id_1, deadline_1)

    ##TODO: remove it later
    # dax2 = '..\\..\\resources\\' + 'CyberShake_30' + '.xml'
    # path = '..\\..\\resources\\saved_schedules\\' + 'CyberShake_30_bundle' + '.json'
    # bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1))
    resources = bundle.dedicated_resources
    transferMx = bundle.transfer_mx
    ideal_flops = bundle.ideal_flops
    ga_initial_schedule = bundle.ga_schedule
    initial_ga_makespan = Utility.get_the_last_time(ga_initial_schedule)
    #print("Initial GA makespan: " + str(initial_ga_makespan))
    ## TODO: end

    ##======================
    ## create realibility
    ##======================
    nodes = HeftHelper.to_nodes(resources)
    ## give 100% to all
    realibility_map = {node.name: 0.75 for node in nodes}
    ## choose one node and give 75% to it
    selected_node = list(nodes)[1]
    #realibility_map[selected_node.name] = 0.95

    ##======================
    ## create heft_executor
    ##======================
    estimator = ExperimentEstimator(transferMx, ideal_flops, realibility_map)
    resource_manager = ExperimentResourceManager(resources)


    ga_machine = GAExecutor(wf,
                            resource_manager,
                            estimator,
                            ga_initial_schedule,
                            base_fail_duration=40,
                            base_fail_dispersion=1)
    ##dynamic_heft = DynamicHeft(wf, resource_manager, estimator)
    ##heft_machine = HeftExecutor(heft_planner=dynamic_heft,
    ##                            base_fail_duration=40,
    ##                            base_fail_dispersion=1)

    ga_machine.init()
    ga_machine.run()

    resulted_schedule = ga_machine.current_schedule
    makespan = Utility.get_the_last_time(resulted_schedule)
    seq_time_validaty = Utility.validateNodesSeq(resulted_schedule)
    dependency_validaty = Utility.validateParentsAndChildren(resulted_schedule, wf)
    ##periods_validaty = Utility.validateUnavailabilityPeriods(ga_executor.schedule, unavailability_periods)
    ##print("heft_makespan: " + str(dynamic_heft_makespan))
    if not is_silent:
        print("=============Res Results====================")
        print("              Makespan %s" % str(makespan))
        print("          Seq validaty %s" % str(seq_time_validaty))
        print("   Dependancy validaty %s" % str(dependency_validaty))
    ##print("      Periods validaty %s" % str(periods_validaty))

    ## 1. obtain ga_schedule
    ## 2. create ga_executor and run experiment there
    ## 3. run 5 times

    ## 3. create heft_executor and run experiment there
    ## 4. run 5 times

    ## 5. compare static ga vs dynamic-ga vs dynamic-heft
    return makespan
    pass

## Single fire

# wf_start_id_1 = "00"
# task_postfix_id_1 = "00"
# deadline_1 = 1000
# wf_name = 'CyberShake_30'
# dax2 = '..\\..\\resources\\' + wf_name + '.xml'
# path = '..\\..\\resources\\saved_schedules\\' + wf_name + '_bundle' + '.json'
# bundle = Utility.load_schedule(path, Utility.readWorkflow(dax2, wf_start_id_1, task_postfix_id_1, deadline_1))
#
# main(False, wf_name, bundle)

#==============================
# uncomment it to use it later
#==============================
# wf_name = 'CyberShake_30'
# n = 100
# result = [main(True, wf_name ) for i in range(n)]
# mx_time = max(result)
# min_time = min(result)
# avr_time = sum(result)/n
# avr_dispersion = math.sqrt(sum([math.pow(abs(res - avr_time), 2) for res in result]))
# print("==============common results================")
# print("           Max: " + str(mx_time))
# print("           Min: " + str(min_time))
# print("           Avr: " + str(avr_time))
# print("     Mean diff: " + str(avr_dispersion))
