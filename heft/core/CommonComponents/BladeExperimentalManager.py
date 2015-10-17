from heft.core.environment.BaseElements import Resource, Node
from heft.core.environment.ResourceManager import Estimator
from heft.core.environment.ResourceManager import ResourceManager


class ExperimentEstimator(Estimator):
    """
    Transfer time between 2 nodes in one blade = transfer_nodes, otherwise = transfer_blades
    """
    def __init__(self, ideal_flops, reliability=1.0, transfer_nodes=1, transfer_blades=100):
        """
        transfer_matrix deleted
        """
        self.cache = dict()
        self.ideal_flops = ideal_flops
        self.reliability = reliability
        self.transfer_nodes = transfer_nodes
        self.transfer_blades = transfer_blades

    # #get estimated time of running the task on the node
    def estimate_runtime(self, task, node):
        result = (task.runtime * self.ideal_flops) / node.flops
        return result

    ## estimate transfer time between node1 and node2 for data generated by the task
    def estimate_transfer_time(self, node1, node2, task1, task2):
        # TODO transfer time should depends on a task data size
        # TODO should be refactored, because now comparing of resources making by names of resources!!!
        if node1 == node2:
            #res = self.transfer_nodes
            res = 0
        else:
            res = self.transfer_blades
        """
        print("Node1 => " + node1.resource.name)
        print("Node2 => " + node2.resource.name)
        print("res = " + str(res))
        """
        return res

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

        lst = [get_transfer_time(name) for (name, file) in task2.input_files.items()]
        result = sum(lst)
        self.cache[task1][task2] = result
        return result

    ## estimate probability of successful ending of the task on the node
    def estimate_reliability(self, task, node):
        return self.reliability


class TransferCalcExperimentEstimator(Estimator):
    """
    Transfer time between 2 nodes in one blade = transfer_nodes, otherwise = transfer_blades
    """
    def __init__(self, ideal_flops, reliability=1.0, transfer_nodes=1, transfer_blades=100):
        """
        transfer_matrix deleted
        """
        self.cache = dict()
        self.ideal_flops = ideal_flops
        self.reliability = reliability
        self.transfer_nodes = transfer_nodes
        self.transfer_blades = transfer_blades

    # #get estimated time of running the task on the node
    def estimate_runtime(self, task, node):
        result = (task.runtime * self.ideal_flops) / node.flops
        return result

    ## estimate transfer time between node1 and node2 for data generated by the task
    def estimate_transfer_time(self, node1, node2, task1, task2):
        # TODO transfer time should depends on a task data size
        # TODO should be refactored, because now comparing of resources making by names of resources!!!

        if node1 == node2:
            #res = self.transfer_nodes
            res = 0
        else:

            transfer_time = 0
            for filename, file in task2.input_files.items():
                if filename in task1.output_files:
                    transfer_time += (file.size / self.transfer_nodes)
            res = transfer_time
            #res = self.transfer_blades
        """
        print("Node1 => " + node1.resource.name)
        print("Node2 => " + node2.resource.name)
        print("res = " + str(res))
        """
        return res

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

        lst = [get_transfer_time(name) for (name, file) in task2.input_files.items()]
        result = sum(lst)
        self.cache[task1][task2] = result
        return result

    ## estimate probability of successful ending of the task on the node
    def estimate_reliability(self, task, node):
        return self.reliability

## TODO: reimplement with inheritance ( regular ExperimentResourceManager)
class ExperimentResourceManager(ResourceManager):

    def setVMParameter(self, rules_list):
        """
        Established farm_capacity and max resource_capacity for each resource
        """
        if len(self.resources) != len(rules_list):
            print("Resources count not equal to rules_list length")
        for res, rule in zip(self.resources, rules_list):
            res.farm_capacity = rule[0]
            res.max_resource_capacity = rule[1]

    def __init__(self, resources):
        self.resources = resources
        self.resources_map = {res.name: res for res in self.resources}
        self._name_to_node = None

    # # TODO: fix problem with id
    def node(self, node):
        if isinstance(node, Node):
            result = [nd for nd in self.resources_map[node.resource.name].nodes if nd.name == node.name]
        else:
            name = node
            result = [nd for nd in self.get_nodes() if nd.name == name]

        if len(result) == 0:
            return None
        return result[0]

    def resource(self, resource):
        return self.res_by_id(resource)

    ##get all resources in the system
    def get_resources(self):
        return self.resources

    def get_live_resources(self):
        resources = self.get_resources()
        result = set()
        for res in resources:
            if res.state != 'down':
                result.add(res)
        return result

    def get_live_nodes(self):
        resources = [res for res in self.get_resources() if res.state != 'down']
        result = set()
        for resource in resources:
            for node in resource.nodes:
                if node.state != "down":
                    result.add(node)
        return result

    def get_all_nodes(self):
        result = set()
        for res in self.resources:
            for node in res.nodes:
                result.add(node)
        return result

    def change_performance(self, node, performance):
        ##TODO: rethink it
        self.resources[node.resource][node].flops = performance

    def byName(self, name):
        if self._name_to_node is None:
            self._name_to_node = {n.name: n for n in self.get_nodes()}
        return self._name_to_node.get(name, None)

    def res_by_id(self, id):
        name = id.name if isinstance(id, Resource)else id
        return self.resources_map[name]

    def get_res_by_name(self, name):
        """
        find resource from resource list by name
        """
        for res in self.resources:
            if res.name == name:
                return res
        return None

    def get_node_by_name(self, name):
        """
        find node in all resources by name
        """
        for res in self.resources:
            for node in res.nodes:
                if node.name == name:
                    return node
        return None






