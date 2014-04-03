from core.CommonComponents.failers.FailBase import FailBase
import random

class FailRandom(FailBase):
    def _check_fail(self, task, node):
        reliability = self.heft_planner.estimator.estimate_reliability(task, node)
        res = random.random()
        if res > reliability:
            return True
        return False
    pass