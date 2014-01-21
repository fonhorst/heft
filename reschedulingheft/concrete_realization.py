from environment.ResourceManager import ResourceManager
from environment.ResourceManager import Estimator

class ExperimentEstimator(Estimator):
    def __init__(self, transferMx):
        self.transfer_matrix = transferMx
        self.cache = dict()

    ##get estimated time of running the task on the node
    def estimate_runtime(self, task, node):
        return task.runtime/node.flops

    ## estimate transfer time between node1 and node2 for data generated by the task
    def estimate_transfer_time(self, node1, node2, task1, task2):
        ##TODO: repair it later
        per_unit_of_time = 1##self.transfer_matrix[node1][node2]
        return self.get_or_estimate(task1, task2)/per_unit_of_time

    def get_or_estimate(self, task1, task2):
        if self.cache.get(task1, None) is not None:
            if self.cache[task1].get(task2, None) is not None:
               return self.cache[task1][task2]
            else:
                self.cache[task1][task2] = None
        else:
            self.cache[task1] = {task2: None}
        def get_transfer_time(name):
            file = task1.output_files.get(name, None)
            return 0 if file is None else file.size

        list = [get_transfer_time(name) for (name, file) in task2.input_files.items()]
        result = sum(list)
        self.cache[task1][task2] = result
        return result

    ## estimate probability of successful ending of the task on the node
    def estimate_reliability(self, task, node):
        pass

class ExperimentResourceManager(ResourceManager):
    def __init__(self, resources):
        self.resources = resources

    ##get all resources in the system
    def get_resources(self):
        return self.resources
