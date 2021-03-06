import math

from heft.core.CommonComponents.failers.FailBase import FailBase

#TODO: detalize how constructor under inheritance works in Python
# TODO: it is possible to do metafactory here. Consider this opportunity later.
class ConcreteNodeFailOnce(FailBase):
    _OP =None

    def _check_fail(self, task, node):

        if self.failed_once is not True:

            node_to_fail = self._OP(self.current_schedule.mapping.keys(), key=lambda x: x.flops)
            length = len(self.current_schedule.mapping[node_to_fail])

            if length == 0:
                # we couldn't fail in this circumstances
                # so, we just miss it
                print("Fail on target node cannot be performed due to there is no tasks")
                self.failed_once = True
                return False

                #raise Exception("Target node for fail hasn't any task")

            l = math.floor(length * self.fail_percent)
            task_id_to_fail = self.current_schedule.mapping[node_to_fail][l].job.id

            if node.name == node_to_fail.name and task.id == task_id_to_fail:
                self.failed_once = True
                return True
        return False

class WeakestFailOnce(ConcreteNodeFailOnce):
    _OP = min
    pass

class StrongestFailOnce(ConcreteNodeFailOnce):
    _OP = max
    pass


