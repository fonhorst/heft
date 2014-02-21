## TODO: move to experiments module

import os
from core.DSimpleHeft import DynamicHeft
from core.comparisons.ComparisonBase import ComparisonUtility
from core.examples.BaseExecutorExample import BaseExecutorExample
from core.executors.GaHeftExecutor import GaHeftExecutor
from core.executors.HeftExecutor import HeftExecutor
from environment.Utility import Utility


class SingleFailGaHeftExecutor(GaHeftExecutor):
    def __init__(self,
                 heft_planner,
                 base_fail_duration,
                 base_fail_dispersion,
                 fixed_interval_for_ga,
                 logger=None,
                 task_id_to_fail=None):
        super().__init__(heft_planner, base_fail_duration, base_fail_dispersion, fixed_interval_for_ga, logger)
        self.task_id_to_fail = task_id_to_fail
        self.is_already_failed = False
        pass

    def _check_fail(self, task, node):
        if not self.is_already_failed and task.id == self.task_id_to_fail:
            self.is_already_failed = True
            return True
        return False
    pass

class SingleFailHeftExecutor(HeftExecutor):
    def __init__(self, heft_planner, base_fail_duration, base_fail_dispersion ,
                 initial_schedule = None, logger=None, task_id_to_fail=None, failure_coeff=0.2):
        super().__init__(heft_planner, base_fail_duration, base_fail_dispersion, initial_schedule, logger)
        self.task_id_to_fail = task_id_to_fail
        self.is_already_failed = False
        self.failure_coeff = failure_coeff
        pass
    def _check_fail(self, task, node):
        if not self.is_already_failed and task.id == self.task_id_to_fail:
            self.is_already_failed = True
            return True
        return False

    def _generate_failtime_and_duration(self, item):
        failtime = round((item.end_time - self.current_time)*self.failure_coeff, 5)
        return (failtime, self.base_fail_duration)
    pass

class SingleFailHeftExecutorExample(BaseExecutorExample):
    def __init__(self):
        pass

    def main(self, reliability, is_silent, wf_name, logger=None, task_id_to_fail=None, failure_coeff=0.2):

        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(None)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)

        ##TODO: look here ! I'm an idiot tasks of wf != tasks of initial_schedule
        dynamic_heft = DynamicHeft(wf, resource_manager, estimator)
        heft_machine = SingleFailHeftExecutor(heft_planner=dynamic_heft,
                                    base_fail_duration=40,
                                    base_fail_dispersion=1,
                                    #initial_schedule=None)
                                    initial_schedule=initial_schedule,
                                    logger=logger,
                                    task_id_to_fail=task_id_to_fail,
                                    failure_coeff=failure_coeff)
        heft_machine.init()
        heft_machine.run()

        ## TODO: remove it later.
        if logger is not None:
            logger.flush()

        seq_time_validaty = Utility.validateNodesSeq(heft_machine.current_schedule)
        dependency_validaty = Utility.validateParentsAndChildren(heft_machine.current_schedule, wf)
        transfer_dependency_validaty = Utility.static_validateParentsAndChildren_transfer(heft_machine.current_schedule, wf, estimator)

        if seq_time_validaty is not True:
            raise Exception("seq_time_validaty failed. taskid=" + str(task_id_to_fail))
        if dependency_validaty is not True:
            raise Exception("dependency_validaty failed. taskid=" + str(task_id_to_fail))
        if transfer_dependency_validaty is not True:
            raise Exception("transfer_dependency_validaty failed. taskid=" + str(task_id_to_fail))


        (makespan, vl1, vl2) = self.extract_result(heft_machine.current_schedule, is_silent, wf)
        return makespan

class SingleFailGaHeftExecutorExample(BaseExecutorExample):
    def __init__(self, fixed_interval_for_ga=15, logger=None):
        self.fixed_interval_for_ga = fixed_interval_for_ga
        pass

    def main(self, reliability, is_silent, wf_name, logger=None, task_id_to_fail=None):
        wf = self.get_wf(wf_name)
        bundle = self.get_bundle(None)
        (estimator, resource_manager, initial_schedule) = self.get_infrastructure(bundle, reliability, False)

        dynamic_heft = DynamicHeft(wf, resource_manager, estimator)
        ga_heft_machine = SingleFailGaHeftExecutor(
                            heft_planner=dynamic_heft,
                            base_fail_duration=40,
                            base_fail_dispersion=1,
                            fixed_interval_for_ga=self.fixed_interval_for_ga,
                            logger=logger,
                            task_id_to_fail=task_id_to_fail)

        ga_heft_machine.init()
        ga_heft_machine.run()

        resulted_schedule = ga_heft_machine.current_schedule
        (makespan, vl1, vl2) = self.extract_result(resulted_schedule, is_silent, wf)
        return makespan

def save_result(f, list_results, id):
    f.write("task_id: " + str(id) + "\n")
    for res in list_results:
        f.write("\t" + str(res) + "\n")
        pass
    f.flush()
    pass

wf_name = "Montage_25"

ids = ["ID000" + (str(i) if len(str(i)) == 2 else "0" + str(i)) + "_000" for i in range(25)]

base_dir = "../../resources/singlefailexps/"

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

heft_name = "HEFT"
heft_path = base_dir + "[{0}]_[{1}]_[{2}].txt".format(wf_name, heft_name, ComparisonUtility.cur_time())
heft_f = open(heft_path, "w")
heft_f.write("alg_name: " + heft_name + "\n")
heft_f.write("wf_name: " + str(wf_name) + "\n")

# gaheft_name = "GAHEFT"
# gaheft_path = base_dir + "[{0}]_[{1}]_[{2}].txt".format(wf_name, gaheft_name, ComparisonUtility.cur_time())
# gaheft_f = open(gaheft_path, "w")
# gaheft_f.write("alg_name: " + gaheft_name + "\n")
# gaheft_f.write("wf_name: " + str(wf_name) + "\n")

n = 20

#     ## reliability 0.95 doesn't matter anything in this case# for id in ids[6:7]:
failure_coeffs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]


## reliability 0.95 doesn't matter anything in this case
heft_makespans = [SingleFailHeftExecutorExample().main(0.95, True, wf_name, None, ids[6], coeff) for coeff in failure_coeffs]
save_result(heft_f, heft_makespans, id)

#     heft_makespans = [SingleFailHeftExecutorExample().main(0.95, True, wf_name, None, id) for i in range(n)]
#     save_result(heft_f, heft_makespans, id)
#
#     # gaheft_makespans = [SingleFailGaHeftExecutorExample().main(0.95, True, wf_name, None, id) for i in range(n)]
#     # save_result(gaheft_f, gaheft_makespans, id)
#     pass
heft_f.close()
# gaheft_f.close()

