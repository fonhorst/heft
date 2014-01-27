from random import Random

##just an enum
import math
from reschedulingheft.HeftHelper import HeftHelper


class SoftItem:
    windows = "windows"
    unix = "unix"
    matlab = "matlab"
    ANY_SOFT = "any_soft"


class Resource:
    def __init__(self, name):
        self.name = name
        self.nodes = set()


class Node:
    Down = "down"
    Unknown = "unknown"
    Static = "static"
    Busy = "busy"
    def __init__(self, name, resource, soft):
        self.name = name
        self.soft = soft
        self.resource = resource
        self.flops = 0
        self.state = Node.Unknown

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name



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
                 ##node.flops = self.rand(random, self.min_flops, self.max_flops)

                 if j == 0:
                     node.flops = 10
                 if j == 1:
                     node.flops = 15
                 if j == 2:
                     node.flops = 25
                 if j == 3:
                     node.flops = 30

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
                nodeCount = 5
            elif i == 1:
                nodeCount = 4
            elif i == 2:
                nodeCount = 3

            for j in range(0, nodeCount):
                node = Node(res.name + "_node_" + str(j), res, [SoftItem.ANY_SOFT])
                if j == 0:
                     node.flops = 10
                if j == 1:
                     node.flops = 10
                if j == 2:
                     node.flops = 25
                if j == 3:
                     node.flops = 25
                if j == 4:
                     node.flops = 30

        nodes = HeftHelper.to_nodes(resources)
        reliability_map = {node.name: 0.75 for node in nodes}

        def probability_estimator(dt, comp_estimation, transfer_estimation):
            M = comp_estimation + transfer_estimation
            sigma = 0.1 * M
            return math.erf((dt - M)/sigma)
        return (resources, reliability_map, probability_estimator)


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
             return  0 if node.name == nd.name else self.rand(random, self.min_transfer_speed, self.max_transfer_speed)
         for node in allNodes:
             transferMx[node.name] = {nd.name: gen(node, nd) for nd in allNodes}
         return transferMx

class User:
    def __init__(self):
        self.name = ""


##just an enum  now
class AccessMode:
    monopolic = "monopolic"
    common = "common"
    restricted = "restricted"

##interface
class PolicyChecker:
    def __init__(self):
        pass

    def get_access_mode(self, wf):
        pass


class Workflow:
    def __init__(self, id, head_task):
        self.id = id
        self.owner = None ## here must be a user
        self.head_task = head_task ## tasks here
        self.deadline = None ## deadline time
        self.deadline_type = None ## deadline type
        self.priority = None ## priority of wf

    def get_task_count(self):
        def add_tasks(unique_tasks, task):
            unique_tasks.update(task.children)
            for child in task.children:
                add_tasks(unique_tasks, child)
        unique_tasks = set()
        if self.head_task is None:
            result = 0
        else:
            add_tasks(unique_tasks, self.head_task)
            result = len(unique_tasks)
        return result


class Task:
    def __init__(self, id, internal_wf_id):
        self.id = id
        self.internal_wf_id = internal_wf_id
        self.wf = None
        self.parents = set() ## set of parents tasks
        self.children = set() ## set of children tasks
        self.soft_reqs = set() ## set of soft requirements
        self.runtime = None ## flops for calculating
        self.input_files = None ##
        self.output_files = None

    def __str__(self):
        return self.id

    def __repr__(self):
        return self.id

class File:
     def __init__(self, name, size):
         self.name = name
         self.size = size

UP_JOB = Task("up_job","up_job")
DOWN_JOB = Task("down_job", "down_job")
