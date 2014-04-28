import math
from core.CommonComponents.failers.FailBase import FailBase
#TODO: detalize how constructor under inheritance works in Python
# TODO: it is possible to do metafactory here. Consider this opportunity later.
class ConcreteNodeFailOnce(FailBase):
    _OP =None
    def _check_fail(self, task, node):
        if self.failed_once is not True:

            min_node = self._OP(self.current_schedule.mapping.keys(), key=lambda x: x.flops)
            l = math.floor(len(self.current_schedule.mapping[min_node]) * self.fail_percent)
            task_id_to_fail = self.current_schedule.mapping[min_node][l].job.id

            if task.id == task_id_to_fail:
                self.failed_once = True
                return True
        return False

class WeakestFailOnce(ConcreteNodeFailOnce):
    _OP = min
    pass

class StrongestFailOnce(ConcreteNodeFailOnce):
    _OP = max
    pass


