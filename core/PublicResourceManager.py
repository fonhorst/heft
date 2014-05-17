from environment.BaseElements import SoftItem, Node
from core.HeftHelper import HeftHelper


class PublicResourceManager:
    # public_resources_manager:
            #   determine nodes of proper soft type
            #   check and determine free nodes
            #   determine reliability of every nodes
            #   determine time_of_execution probability for (task,node) pair

    def __init__(self, public_resources, reliability_map, probability_estimator):
        self.public_resources = public_resources
        self.reliability_map = reliability_map
        self.probability_estimator = probability_estimator

        self.busies_nodes = set()

    ## get available nodes by soft type
    def get_by_softreq(self, soft_reqs):
        nodes = HeftHelper.to_nodes(self.public_resources)
        def check_reqs(node):
            return (soft_reqs in node.soft) or (SoftItem.ANY_SOFT in node.soft)
        gotcha = [node for node in nodes if node.state != Node.Down and check_reqs(node)]
        return gotcha

    def isBusy(self, node):
        return node.name in self.busies_nodes

    def checkBusy(self, node, is_busy):
        if not is_busy:
            self.busies_nodes.remove(node.name)
        else:
            self.busies_nodes.add(node.name)

    def checkDown(self, node_name, is_down):
        nodes = HeftHelper.to_nodes(self.public_resources)
        for nd in nodes:
            if nd.name == node_name:
                if is_down:
                    nd.state = Node.Down
                else:
                    nd.state = Node.Unknown
        pass


    def get_reliability(self, node_name):
        return self.reliability_map[node_name]

    def isCloudNode(self, node):
        result = node.name in [nd.name for nd in HeftHelper.to_nodes(self.public_resources)]
        return result






