from copy import copy, deepcopy
import functools

##just an enum
from heft.algs.heft import HeftHelper


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


class Workflow:
    def __init__(self, id, name, head_task):
        self.id = id
        self.name = name
        self.head_task = head_task

        self._unique_tasks = None
        self._id_to_task = None
        self._parent_child_dict = None

    def get_task_count(self):
        unique_tasks =self.get_all_unique_tasks()
        result = len(unique_tasks)
        return result

    def get_all_unique_tasks(self):
        """
        Get all unique tasks in sorted order
        """
        if self._unique_tasks is None:
            def add_tasks(unique_tasks, task):
                unique_tasks.update(task.children)
                for child in task.children:
                    add_tasks(unique_tasks, child)
            unique_tasks = set()
            if self.head_task is None:
                result = []
            else:
                add_tasks(unique_tasks, self.head_task)
                result = unique_tasks
            self._unique_tasks = sorted(result, key=lambda x: x.id)
        return copy(self._unique_tasks)

    def get_tasks_id(self):
        return [t.id for t in self._unique_tasks]

    def byId(self, id):
        if self._id_to_task is None:
            self._id_to_task = {t.id: t for t in self.get_all_unique_tasks()}
        return self._id_to_task.get(id, None)

    def is_parent_child(self, id1, id2):
        if self._parent_child_dict is None:
            self._build_ancestors_map()
        return (id2 in self._parent_child_dict[id1]) or (id1 in self._parent_child_dict[id2])

    def ancestors(self, id):
        if self._parent_child_dict is None:
            self._build_ancestors_map()
        return self._parent_child_dict[id]

    ## TODO: for one-time use. Remove it later.
    def avr_runtime(self, package_name):
        tsks = [tsk for tsk in HeftHelper.get_all_tasks(self) if package_name in tsk.soft_reqs]
        common_sum = sum([tsk.runtime for tsk in tsks])
        return common_sum/len(tsks)


    def _build_ancestors_map(self):
        self._parent_child_dict = {}
        def build(el):
            if el.id in self._parent_child_dict:
                return self._parent_child_dict[el.id]
            if len(el.children) == 0:
                res = []
            else:
                all_ancestors = [[c.id for c in el.children]] + [build(c) for c in el.children]
                res = functools.reduce(lambda seed, x: seed + x, all_ancestors, [])
            self._parent_child_dict[el.id] = res
            return res
        build(self.head_task)
        self._parent_child_dict = {k: set(v) for k, v in self._parent_child_dict.items()}


class Task:
    def __init__(self, id, internal_wf_id, is_head=False):
        self.id = id
        self.internal_wf_id = internal_wf_id
        self.wf = None
        self.parents = set() ## set of parents tasks
        self.children = set() ## set of children tasks
        self.soft_reqs = set() ## set of soft requirements
        self.runtime = None ## flops for calculating
        self.input_files = None ##
        self.output_files = None
        self.is_head = is_head

    def __str__(self):
        return self.id

    def __repr__(self):
        return self.id

class File:
     def __init__(self, name, size):
         self.name = name
         self.size = size

UP_JOB = Task("up_job", "up_job")
DOWN_JOB = Task("down_job", "down_job")
