from heft.utilities.common import trace


class FailBase:
    #@trace
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _check_fail(self, task, node):
        pass
