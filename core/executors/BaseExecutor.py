from core.executors.EventMachine import EventMachine, TaskStart, TaskFinished, NodeFailed, NodeUp


class BaseExecutor(EventMachine):
    def event_arrived(self, event):
        if isinstance(event, TaskStart):
            self._task_start_handler(event)
            return
        if isinstance(event, TaskFinished):
            self._task_finished_handler(event)
            return
        if isinstance(event, NodeFailed):
            self._node_failed_handler(event)
            return
        if isinstance(event, NodeUp):
            self._node_up_handler(event)
            return
        raise Exception("Unknown event: " + str(event))

    def _task_start_handler(self, event):
        pass

    def _task_finished_handler(self, event):
        pass

    def _node_failed_handler(self, event):
        pass

    def _node_up_handler(self, event):
        pass

    pass
