from random import Random

##just an enum
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



class ResourceGenerator:

     MIN_RES_COUNT = 15
     MAX_RES_COUNT = 16
     MIN_NODE_COUNT = 15
     MAX_NODE_COUNT = 16

     MIN_FLOPS = 5
     MAX_FLOPS = 20

     MIN_TRANSFER_SPEED = 1*1024*1024 ## 1MB
     MAX_TRANSFER_SPEED = 1*1024*1024*1024 ## 1GB

     def __init__(self):
         pass

     def generate(self):
         random = Random()
         resCount = random.randint(ResourceGenerator.MIN_RES_COUNT, ResourceGenerator.MAX_RES_COUNT)
         resources = list()
         for i in range(0,resCount):
             res = Resource("res_" + str(i))
             resources.append(res)
             nodeCount = random.randint(ResourceGenerator.MIN_NODE_COUNT, ResourceGenerator.MAX_NODE_COUNT)
             for j in range(0,nodeCount):
                 node = Node( res.name +  "_node_" + str(j), res, [SoftItem.ANY_SOFT])
                 node.flops = random.randint(ResourceGenerator.MIN_FLOPS, ResourceGenerator.MAX_FLOPS)
                 res.nodes.add(node)
         return resources

     def generateTransferMatrix(self, resources):
         random = Random()
         allNodes = list()
         for res in resources:
             for node in res.nodes:
                 allNodes.append(node)
         transferMx = dict()
         def gen(node, nd):
             return  0 if node.name == nd.name else random.randint(ResourceGenerator.MIN_TRANSFER_SPEED, ResourceGenerator.MAX_TRANSFER_SPEED)
         for node in allNodes:
             transferMx[node.name] = {nd.name: gen(node,nd)  for nd in allNodes}
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
