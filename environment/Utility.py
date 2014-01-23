from environment.DAXParser import DAXParser
from random import Random

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




















