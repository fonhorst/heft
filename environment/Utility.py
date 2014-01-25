import json
from environment.DAXParser import DAXParser
from random import Random
from environment.Resource import Node
from environment.ResourceManager import ScheduleItem, Schedule


def reverse_dict(d):
    """ Reverses direction of dependence dict
    >>> d = {'a': (1, 2), 'b': (2, 3), 'c':()}
    >>> reverse_dict(d)
    {1: ('a',), 2: ('a', 'b'), 3: ('b',)}
    """
    result = {}
    for key in d:
        for val in d[key]:
            result[val] = result.get(val, tuple()) + (key, )
    return result

class Utility:

    MIN_PIPELINE_SIZE = 10
    MAX_PIPELINE_SIZE = 40

    def __init__(self):
         pass

    @staticmethod
    def generateUrgentPipeline(dax_filepath, wf_start_id, task_postfix_id, deadline):
        parser = DAXParser()
        random = Random()
        pipelineSize = 1##random.randint(Utility.MIN_PIPELINE_SIZE,Utility.MAX_PIPELINE_SIZE)
        wfs = [parser.parseXml(dax_filepath,wf_start_id + str(i), task_postfix_id + str(i)) for i in range(0, pipelineSize)]
        for wf in wfs:
            wf.deadline = deadline
        return wfs

    @staticmethod
    def readWorkflow(dax_filepath, wf_start_id, task_postfix_id, deadline):
        parser = DAXParser()
        wf = parser.parseXml(dax_filepath, wf_start_id + "0", task_postfix_id + "0")
        wf.deadline = deadline
        return wf

    @staticmethod
    def validateNodesSeq(schedule):
        for (node, items) in schedule.mapping.items():
            time = -1
            for item in items:
                if time > item.start_time:
                    return False
                    ##raise Exception("Node: " + str(node) + " all time: " + str(time) + " st_time: " + str(item.start_time))
                else:
                    time = item.start_time
                if time > item.end_time:
                    return False
                else:
                    time = item.end_time
        return True

    @staticmethod
    def validateParentsAndChildren(schedule, workflow):
        #{
        #   task: (node,start_time,end_time),
        #   ...
        #}
        task_to_node = dict()
        for (node, items) in schedule.mapping.items():
            for item in items:
                task_to_node[item.job.id] = (node, item.start_time, item.end_time)

        def check(task):
            for child in task.children:
                p_end_time = task_to_node[task.id][2]
                c_start_time = task_to_node[child.id][1]
                if c_start_time < p_end_time:
                    return False
                res = check(child)
                if res is False:
                    return False
            return True

        for task in workflow.head_task.children:
            res = check(task)
            if res is False:
                    return False
        return True

    @staticmethod
    def get_the_last_time( schedule):
        def get_last_time(node_items):
            return 0 if len(node_items) == 0 else node_items[-1].end_time
        last_time = max([get_last_time(node_items) for (node, node_items) in schedule.mapping.items()])
        return last_time

    @staticmethod
    def build_schedule_decoder(head_task, resources):
        res_dict = {res.name: res for res in resources}

        def get_all_tasks(task, all):
            for child in task.children:
                all.add(child)
                get_all_tasks(child, all)
            return all

        task_dict = {t.id: t for t in get_all_tasks(head_task, set())}

        def as_schedule(dct):
            if '__cls__' in dct and dct['__cls__'] == 'Node':
                res = res_dict[dct['resource']]
                node = Node(dct['name'], res, dct['soft'])
                node.flops = dct['flops']
                return node
            if '__cls__' in dct and dct['__cls__'] == 'ScheduleItem':
                task = task_dict[dct['job']]
                scItem = ScheduleItem(task, dct['start_time'], dct['end_time'])
                scItem.state = dct['state']
                return scItem
            if '__cls__' in dct and dct['__cls__'] == 'Schedule':
                mapping = {node_values['node']: node_values['value'] for node_values in dct['mapping']}
                schedule = Schedule(mapping)
                return schedule
            return dct

        return as_schedule
    pass

class ScheduleEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Schedule):
                return {'__cls__': 'Schedule', 'mapping': [
                    {'node': self.default(node),
                      'value': [self.default(el) for el in values]}
                     for (node, values) in obj.mapping.items()]}
            if isinstance(obj, ScheduleItem):
                return {'__cls__': 'ScheduleItem', 'job': obj.job.id, 'start_time': obj.start_time,
                        'end_time': obj.end_time, 'state': obj.state}
            # Let the base class default method raise the TypeError
            if isinstance(obj, Node):
                return {'__cls__': 'Node', 'name': obj.name, 'soft': obj.soft,
                        'resource': obj.resource.name, 'flops': obj.flops}
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, obj)




















