from copy import copy
import functools
import sys
import uuid
import random
# #just an enum


class SoftItem:
    windows = "windows"
    unix = "unix"
    matlab = "matlab"
    ANY_SOFT = "any_soft"


class Resource:

    Down = "down"
    Unknown = "unknown"
    Static = "static"
    Busy = "busy"
    def __init__(self, name, nodes=None):
        self.name = name
        if nodes is None:
            self.nodes = set()
        else:
            self.nodes = nodes
        self.state = Resource.Unknown

    def get_live_nodes(self):
        result = set()
        for node in self.nodes:
            if node.state != Node.Down:
                result.add(node)
        return result

    def get_cemetery(self):
        result = set()
        for node in self.nodes:
            if node.state == Node.Down:
                result.add(node)
        return result

    def __eq__(self, other):
        if isinstance(other, Resource) and self.name == other.name:
            return True
        else:
            return super().__eq__(other)

    def __hash__(self):
        return hash(self.name)


class Node:

    Down = "down"
    Unknown = "unknown"
    Static = "static"
    Busy = "busy"

    def __init__(self, name, resource, soft, flops=0):
        self.name = name
        self.soft = soft
        self.resource = resource
        self.flops = flops
        self.state = Node.Unknown
        self.id = uuid.uuid4()

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self.name)

    def __eq__(self, other):
        if isinstance(other, Node) and self.name == other.name:
            return True
        else:
            return super().__eq__(other)

    def __hash__(self):
        return hash(self.name)


class SubWorkflow:
    def __init__(self, id, name, head_task):
        self.id = id
        self.name = name
        self.head_task = head_task

    def get_real_wf(self):
        task = self.head_task

    def process_task(self, task):
        result = []
        if task.range is not None:
            tasks_number = random.randint(task.range.min, task.range.max)
            for i in range(task.range.min, tasks_number):
                if type(task) is SubWorkflow:
                    result.append(task.get_real_wf())
                else:
                    result.append(task.copy())
        else:
            if type(task) is SubWorkflow:
                result = task.get_real_wf()


class Workflow:
    def __init__(self, id, name, head_task):
        self.id = id
        self.name = name
        self.head_task = head_task
        self.max_sweep = sys.maxsize

        self._unique_tasks = None
        self._id_to_task = None
        self._parent_child_dict = None

    def get_task_count(self):
        unique_tasks = self.get_all_unique_tasks()
        result = len(unique_tasks)
        return result

    def get_max_sweep(self):
        if self.max_sweep == sys.maxsize:
            def find_all_sweep_size(task, calculated):
                max_sweep = 0
                for child in task.children:
                    if child not in calculated:
                        max_sweep = find_all_sweep_size(child, calculated) + max_sweep
                        calc.add(child)

                return max(max_sweep, 1)

            if self.head_task is None:
                self.max_sweep = 0
            else:
                calc = set()
                self.max_sweep = find_all_sweep_size(self.head_task, calc)
        return self.max_sweep

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

    def by_num(self, num):
        numstr = str(num)
        zeros = "".join("0" for _ in range(5 - len(numstr)))
        ## TODO: correct indexation
        id = str.format("ID{zeros}{num}_000", zeros=zeros, num=numstr)
        return self.byId(id)

    def ancestors(self, id):
        if self._parent_child_dict is None:
            self._build_ancestors_map()
        return self._parent_child_dict[id]

    ## TODO: for one-time use. Remove it later.
    # def avr_runtime(self, package_name):
    #     tsks = [tsk for tsk in HeftHelper.get_all_tasks(self) if package_name in tsk.soft_reqs]
    #     common_sum = sum([tsk.runtime for tsk in tsks])
    #     return common_sum / len(tsks)


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


class AbstractWorkflow(Workflow):
    def copy(self):
        result = AbstractWorkflow(self.id, self.name, self.head_task.copy("",True))
        result.max_sweep = sys.maxsize
        result._unique_tasks = self._unique_tasks
        result._id_to_task = self._id_to_task
        result._parent_child_dict = self._parent_child_dict
        return result

    def get_real_wf(self):
        real_wf = self.copy()
        self.head_task = self.head_task.get_real_tasks()
        return real_wf


class Range:
    def __init__(self, min, max):
        self.min = min
        self.max = max


class Task:
    def __init__(self, id, internal_wf_id, is_head=False, alternates=None, subtask=False):
        self.id = id
        self.internal_wf_id = internal_wf_id
        self.wf = None
        self.parents = set()  ## set of parents tasks
        self.children = set()  ## set of children tasks
        self.soft_reqs = set()  ## set of soft requirements
        self.runtime = None  ## flops for calculating
        self.input_files = None  ##
        self.output_files = None
        self.is_head = is_head
        self.alternates = alternates
        self.subtask = subtask

    def copy(self,id="",full=False,children=False):
        if id == "":
            new_id = self.id
        else:
            new_id = self.id + "_" + id
        result = Task(new_id, self.internal_wf_id, self.is_head)
        if not children:
            result.children = self.children
        else:
            result.children = set()
            children_copy = copy(self.children)
            for child in children_copy:
                result.children.add(child.copy(id,full,children))
        result.parents = self.parents
        result.input_files = self.input_files
        result.output_files = self.output_files
        result.runtime = self.runtime
        if full:
            if hasattr(self, 'alternates'):
                result.alternates = self.alternates
            if hasattr(self, 'range'):
                result.range = self.range
        return result

    def __str__(self):
        return self.id

    def __repr__(self):
        return self.id

    def get_list_tasks(self):
        result = []
        for child_task in self.children:
            if len(child_task.children) == 0:
                result.append(child_task)
            list_tasks = child_task.get_list_tasks()
            if len(list_tasks) > 0:
                for list_task in list_tasks:
                    result.append(list_task)
        return result

    def get_real_tasks(self):
        real_task = self.copy("",True)
        children_copy = copy(real_task.children)
        for child_task in children_copy:
            if hasattr(child_task, 'range'):
                result = []
                tasks_number = random.randint(child_task.range.min, child_task.range.max)
                if child_task.alternates is not None:
                    subwf_id = random.randint(0, len(child_task.alternates)-1)
                    subwf = child_task.alternates[subwf_id]
                    real_subwf = subwf.head_task.get_real_tasks()
                    real_subwf.parents = set()
                    for parent_task in child_task.parents:
                        real_subwf.parents.add(parent_task)
                for i in range(0, tasks_number):
                    if child_task.alternates is not None:
                        result.append(real_subwf.copy(str(i),False,True))
                    else:
                        result.append(child_task.copy(str(i)))
                for task in result:
                    for parent_task in child_task.parents:
                        parent_task.children.add(task)
                    if child_task.alternates is not None:
                        subwf_list_tasks = task.get_list_tasks()
                        for subwf_list_task in subwf_list_tasks:
                            for child_child_task in child_task.children:
                                child_child_task.parents.add(subwf_list_task)
                                subwf_list_task.children.add(child_child_task)
                    else:
                        for child_child_task in child_task.children:
                            child_child_task.parents.add(task)
                for child_child_task in child_task.children:
                    if child_task in child_child_task.parents:
                        child_child_task.parents.remove(child_task)
                for parent_task in child_task.parents:
                    parent_task.children.remove(child_task)
            else:
                if child_task.alternates is not None:
                    subwf_id = random.randint(0, len(child_task.alternates)-1)
                    subwf = child_task.alternates[subwf_id]
                    real_subwf = subwf.head_task.get_real_tasks()
                    real_subwf.parents = set()
                    for parent_task in child_task.parents:
                        real_subwf.parents.add(parent_task)
                        parent_task.children.add(real_subwf)
                    subwf_list_tasks = real_subwf.get_list_tasks()
                    for subwf_list_task in subwf_list_tasks:
                        for child_child_task in child_task.children:
                            child_child_task.parents.add(subwf_list_task)
                            subwf_list_task.children.add(child_child_task)

            if child_task.alternates is not None:
                if child_task in real_task.children:
                    real_task.children.remove(child_task)

            child_task.alternates = None
            child_task.get_real_tasks()
        return real_task

    # def __hash__(self):
    #     return hash(self.id)
    #
    # def __eq__(self, other):
    #     if isinstance(other, Task):
    #         return self.id == other.id
    #     else:
    #         return super().__eq__(other)

class File:
    def __init__(self, name, size):
        self.name = name
        self.size = size


UP_JOB = Task("up_job", "up_job")
DOWN_JOB = Task("down_job", "down_job")
