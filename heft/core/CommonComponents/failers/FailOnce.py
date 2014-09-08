import random

from heft.core.CommonComponents.failers.FailBase import FailBase

#TODO: detalize how constructor under inheritance works in Python
class FailOnce(FailBase):

    # estimator = None
    # task_id_to_fail = None

    def _check_fail(self, task, node):
        if self.failed_once is not True:
            reliability = self.estimator.estimate_reliability(task, node)

            failed = False
            if self.task_id_to_fail is None:
                res = random.random()
                failed = res > reliability
            elif self.task_id_to_fail == task.id:
                failed = True
            # r = self.task_id_to_fail == task.id
            # print("Checking failed {0} == {1}: {2} ".format(self.task_id_to_fail, task.id, r))
            if failed is True:
                self.failed_once = True
                return True
        return False
