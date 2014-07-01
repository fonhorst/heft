import math
from random import Random
from algs.heft.HeftHelper import HeftHelper
from core.environment.BaseElements import Resource, Node, SoftItem

__author__ = 'nikolay'


class ResourceGenerator:

     MIN_RES_COUNT = 15
     MAX_RES_COUNT = 16
     MIN_NODE_COUNT = 15
     MAX_NODE_COUNT = 16

     MIN_FLOPS = 5
     MAX_FLOPS = 20

     MIN_TRANSFER_SPEED = 1*1024*1024 ## 1MB
     MAX_TRANSFER_SPEED = 1*1024*1024*1024 ## 1GB

     def __init__(self, min_res_count=MIN_RES_COUNT,
                        max_res_count=MAX_RES_COUNT,
                        min_node_count=MIN_NODE_COUNT,
                        max_node_count=MAX_NODE_COUNT,
                        min_flops = MIN_FLOPS,
                        max_flops = MAX_FLOPS,
                        min_transfer_speed = MIN_TRANSFER_SPEED,
                        max_transfer_speed = MAX_TRANSFER_SPEED):
         self.min_res_count = min_res_count
         self.max_res_count = max_res_count
         self.min_node_count = min_node_count
         self.max_node_count = max_node_count
         self.min_flops = min_flops
         self.max_flops = max_flops
         self.min_transfer_speed = min_transfer_speed
         self.max_transfer_speed = max_transfer_speed

     def generate(self):
         random = Random()
         resCount = self.rand(random, self.min_res_count, self.max_res_count)
         resources = list()
         for i in range(0,resCount):
             res = Resource("res_" + str(i))
             resources.append(res)
             nodeCount = self.rand(random, self.min_node_count, self.max_node_count)
             for j in range(0, nodeCount):
                 node = Node(res.name + "_node_" + str(j), res, [SoftItem.ANY_SOFT])
                 ##TODO: repair it later
                 node.flops = self.rand(random, self.min_flops, self.max_flops)

                 #if j == 0:
                 #     node.flops = 10
                 #if j == 1:
                 #     node.flops = 15
                 #if j == 2:
                 #     node.flops = 25
                 #if j == 3:
                 #     node.flops = 30

                 res.nodes.add(node)
         return resources

     def generate_public_resources(self):
        ## TODO: remake it later
        #(public_resources, generate_reliability, generate_probability_law_for_(task,node)_pair) = generate public_resource
        resCount = 3
        resources = list()
        for i in range(0, resCount):
            res = Resource("public_res_" + str(i))
            resources.append(res)
            nodeCount = None
            if i == 0:
                nodeCount = 15
            elif i == 1:
                nodeCount = 12
            elif i == 2:
                nodeCount = 9

            for j in range(0, nodeCount):
                node = Node(res.name + "_node_" + str(j), res, [SoftItem.ANY_SOFT])
                # if j == 0:
                #      node.flops = 10 + 5
                # if j == 1:
                #      node.flops = 15 + 10#10*3
                # if j == 2:
                #      node.flops = 25 + 10#25*3
                # if j == 3:
                #      node.flops = 25 + 10#25*3
                # if j == 4:
                #      node.flops = 30 + 10#30*3
                # if j == 5:
                #      node.flops = 10 + 5
                # if j == 6:
                #      node.flops = 15 + 10#10*3
                # if j == 7:
                #      node.flops = 25 + 10#25*3
                # if j == 8:
                #      node.flops = 25 + 10#25*3
                # if j == 9:
                #      node.flops = 30 + 10#30*3
                # if j == 10:
                #      node.flops = 10 + 5
                # if j == 11:
                #      node.flops = 15 + 10#10*3
                # if j == 12:
                #      node.flops = 25 + 10#25*3
                # if j == 13:
                #      node.flops = 25 + 10#25*3
                # if j == 14:
                #      node.flops = 30 + 10#30*3

                if j == 0:
                     node.flops = 10
                if j == 1:
                     node.flops = 15#10*3
                if j == 2:
                     node.flops = 25#25*3
                if j == 3:
                     node.flops = 25#25*3
                if j == 4:
                     node.flops = 30#30*3
                if j == 5:
                     node.flops = 10
                if j == 6:
                     node.flops = 15#10*3
                if j == 7:
                     node.flops = 25#25*3
                if j == 8:
                     node.flops = 25#25*3
                if j == 9:
                     node.flops = 30#30*3
                if j == 10:
                     node.flops = 10
                if j == 11:
                     node.flops = 15#10*3
                if j == 12:
                     node.flops = 25#25*3
                if j == 13:
                     node.flops = 25#25*3
                if j == 14:
                     node.flops = 30#30*3

                res.nodes.add(node)

        nodes = HeftHelper.to_nodes(resources)
        reliability_map = {node.name: 0.9 for node in nodes}

        def probability_estimator(dt, comp_estimation, transfer_estimation):
            M = comp_estimation + transfer_estimation
            sigma = 0.1 * M
            result = 0.5 *(1 + math.erf((dt - M)/sigma))
            return result
        return (resources, reliability_map, probability_estimator)

     @staticmethod
     def r(list_flops):
        result = []
        res = Resource("res_0")
        for flop, i in zip(list_flops, range(len(list_flops))):
            node = Node(res.name + "_node_" + str(i), res, [SoftItem.ANY_SOFT])
            node.flops = flop
            result.append(node)
        res.nodes = result
        return [res]

     @staticmethod
     def rand(random, min, max):
         return min if min == max else random.randint(min, max)

     def generateTransferMatrix(self, resources):
         random = Random()
         allNodes = list()
         for res in resources:
             for node in res.nodes:
                 allNodes.append(node)
         transferMx = dict()
         def gen(node, nd):
             return 0 if node.name == nd.name else self.rand(random, self.min_transfer_speed, self.max_transfer_speed)
         for node in allNodes:
             transferMx[node.name] = {nd.name: gen(node, nd) for nd in allNodes}
         return transferMx




