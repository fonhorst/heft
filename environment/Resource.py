__author__ = 'nikolay'


class Resource:
    def __init__(self):
        self.name = ""
        self.nodes = set()


class Node:
    Down = "down"
    Unknown = "unknown"
    Static = "static"
    Busy = "busy"
    def __init__(self):
        self.name = ""
        self.soft = set()
        self.cpu_count = 0
        self.cpu_flops
        self.memory = 0
        self.bandwidth = 0
        self.resource = None
        self.state = Node.Unknown


##just an enum
class SoftItem:
    windows = "windows"
    unix = "unix"
    matlab = "matlab"
    ANY_SOFT = "any_soft"
    ##TODO: complete it later


class User:
    def __init__(self):
        self.name = ""


##just an enum  now
class AccessMode:
    monopolic = "monopolic"
    common = "common"
    restricted = "restricted"


class PolicyChecker:
    def __init__(self):
        pass

    def get_access_mode(self, wf):
        pass


class Workflow:
    def __init__(self):
        self.id = None
        self.owner = None ## here must be a user
        self.head_task = None ## tasks here
        self.deadline = None ## deadline time
        self.deadline_type = None ## deadline type
        self.priority = None ## priority of wf


class Task:
    def __init__(self, id):
        self.id = None
        self.wf = None
        self.parents = set() ## set of parents tasks
        self.children = set() ## set of children tasks
        self.soft_reqs = set() ## set of soft requirements
        self.hardware_reqs = None ## set of hardware requirements
        self.generated_output_size = None ## size of generated output data, needed for transfering

UP_JOB = Task("up_job")
DOWN_JOB = Task("down_job")
