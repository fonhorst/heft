import random

from heft.core.CommonComponents.failers.FailBase import FailBase
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import ScheduleItem
from heft.core.environment.Utility import signal_if_true
from heft.utilities.common import trace


class FailRandom(FailBase):

    #@trace
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fail_count_upper_limit = kwargs.get("fail_count_upper_limit", None)

        self._failed_counts = 0

    @signal_if_true
    def _check_fail(self, task, node):

        if self._fail_count_upper_limit is not None and self._failed_counts >= self._fail_count_upper_limit:
            return False


        #TODO: remake it later
        if hasattr(self, "estimator"):
            reliability = self.estimator.estimate_reliability(task, node)
        else:
            reliability = self.heft_planner.estimator.estimate_reliability(task, node)
        res = random.random()
        if res > reliability:
            self._failed_counts += 1
            print("FAIL {0}/{1}".format(self._failed_counts, self._fail_count_upper_limit))
            return True
        return False
