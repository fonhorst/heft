import json
from environment.DAXParser import DAXParser
from random import Random
from environment.Resource import Node, Resource
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
    def validate_time_seq(items):
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
    def validateNodesSeq(schedule):
        for (node, items) in schedule.mapping.items():
            #time = -1
            #for item in items:
            #    if time > item.start_time:
            #        return False
            #        ##raise Exception("Node: " + str(node) + " all time: " + str(time) + " st_time: " + str(item.start_time))
            #    else:
            #        time = item.start_time
            #    if time > item.end_time:
            #        return False
            #    else:
            #        time = item.end_time
            result = Utility.validate_time_seq(items)
            if result is False:
                return False
        return True

    #@staticmethod
    #def validateParentsAndChildren(schedule, workflow):
    #    #{
    #    #   task: (node,start_time,end_time),
    #    #   ...
    #    #}
    #    task_to_node = dict()
    #    for (node, items) in schedule.mapping.items():
    #        for item in items:
    #            task_to_node[item.job.id] = (node, item.start_time, item.end_time)
    #
    #    def check(task):
    #        for child in task.children:
    #            p_end_time = task_to_node[task.id][2]
    #            c_start_time = task_to_node[child.id][1]
    #            if c_start_time < p_end_time:
    #                return False
    #            res = check(child)
    #            if res is False:
    #                return False
    #        return True
    #
    #    for task in workflow.head_task.children:
    #        res = check(task)
    #        if res is False:
    #                return False
    #    return True

    ## TODO: under development now
    @staticmethod
    def validateParentsAndChildren(schedule, workflow):
        #{
        #   task: (node,start_time,end_time),
        #   ...
        #}
        task_to_node = dict()
        for (node, items) in schedule.mapping.items():
            for item in items:
                seq = task_to_node.get(item.job.id, [])
                seq.append(item)
                ##seq.append(node, item.start_time, item.end_time, item.state)
                task_to_node[item.job.id] = seq

        def check_failed(seq):
            ## in schedule items sequence, only one finished element must be
            ## resulted schedule can contain only failed and finished elements
            states = [item.state for item in seq]
            if states[-1] != ScheduleItem.FINISHED:
                return False
            finished = [state for state in states if state == ScheduleItem.FINISHED]
            if len(finished) != 1:
                return False
            failed = [state for state in states if state == ScheduleItem.FAILED]
            if len(states) - len(finished) != len(failed):
                return False
            return True

        task_to_node = {job_id: sorted(seq, key=lambda x: x.start_time)for (job_id, seq)in task_to_node.items() }
        for (job_id, seq) in task_to_node.items():
            result = Utility.validate_time_seq(seq)
            if result is False:
                return False
            if check_failed(seq) is False:
                return False


        def check(task):
            for child in task.children:
                p_end_time = task_to_node[task.id][-1].end_time
                c_start_time = task_to_node[child.id][-1].start_time
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
    def validateUnavailabilityPeriods(schedule, unavailability_periods):
        ## TODO: refactor it later
        #unavailability_periods =
        # {
        #   node: StartEndPair(start,end),StartEndPair(start1, end1)
        #   ...
        # }

        ## check every seq for proper time interval
        result = False in [Utility.validate_time_seq(seq) for (node, seq) in unavailability_periods.items()]
        if result is True:
            return False

        ## TODO: remake it later
        def check_unavailability(sched_seq, unavil_seq):
            def check(pair):
                pred = None
                next = None
                for item in sched_seq:
                    if (item.start_time >= pair.start_time):
                        next = item
                        break
                    pred = item

                if pred is None and next is None:
                    return True
                if next is None and pred.end_time <= pair.start_time :
                    return True
                if pred.end_time <= pair.start_time and next.start_time >= pair.end_time:
                    return True

                return False

            reslt = False in [check(pair) for pair in unavil_seq]
            if reslt is True:
                return False
            return True

        reslt = False in [check_unavailability(schedule.mapping[node], seq) for (node, seq) in unavailability_periods.items()]
        if reslt is True:
            return False
        return True

    @staticmethod
    def get_the_last_time( schedule):
        def get_last_time(node_items):
            return 0 if len(node_items) == 0 else node_items[-1].end_time
        last_time = max([get_last_time(node_items) for (node, node_items) in schedule.mapping.items()])
        return last_time

    @staticmethod
    def build_bundle_decoder(head_task):
        def get_all_tasks(task, all):
            for child in task.children:
                all.add(child)
                get_all_tasks(child, all)
            return all

        task_dict = {t.id: t for t in get_all_tasks(head_task, set())}

        def as_schedule(dct):
            if '__cls__' in dct and dct['__cls__'] == 'Node':
                res = dct['resource']
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
            if '__cls__' in dct and dct['__cls__'] == 'Resource':
                res = Resource(dct['name'])
                res.nodes = dct['nodes']
                return res
            if '__cls__' in dct and dct['__cls__'] == 'SaveBundle':

                all_nodes = set()
                for res in dct['dedicated_resources']:
                    for node in res.nodes:
                        node.resource = res
                    all_nodes.update(res.nodes)

                all_nodes = {node.name: node for node in all_nodes}

                dct['ga_schedule'].mapping = {all_nodes[node_name]:values for (node_name,values) in dct['ga_schedule'].mapping.items()}

                bundle = SaveBundle(dct['name'],
                                    dct['dedicated_resources'],
                                    dct['transfer_mx'],
                                    dct['ideal_flops'],
                                    dct['ga_schedule'],
                                    dct['wf_name'])
                return bundle
            return dct

        return as_schedule

    @staticmethod
    def save_schedule(path, wf_name, resources, transferMx, ideal_flops, schedule):

        name = wf_name +"_bundle"
        bundle = SaveBundle(name, resources, transferMx, ideal_flops, schedule, wf_name)

        ##'..\\..\\resources\\saved_schedules\\' + name + '.json'
        f = open(path, 'w')
        json.dump(bundle, f, cls=SaveBundleEncoder)
        f.close()
        pass

    @staticmethod
    def load_schedule(path, wf):
        decoder = Utility.build_bundle_decoder(wf.head_task)
        f = open(path, 'r')
        bundle = json.load(f, object_hook=decoder)
        f.close()
        return bundle

    pass
class SaveBundleEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, SaveBundle):
                result = {'__cls__': 'SaveBundle',
                        'name': obj.name,
                        'dedicated_resources': [self.default(el) for el in obj.dedicated_resources],
                        'transfer_mx': self.encode(obj.transfer_mx),
                        'ideal_flops': obj.ideal_flops,
                        'ga_schedule': self.default(obj.ga_schedule),
                        'wf_name': obj.wf_name}
                return result
            if isinstance(obj, Resource):
                return {'__cls__': 'Resource',
                        'name': obj.name,
                        'nodes': [self.default(node) for node in obj.nodes]}
            if isinstance(obj, Schedule):
                return {'__cls__': 'Schedule', 'mapping': [
                    {'node': node.name,
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

class SaveBundle:
    def __init__(self, name, dedicated_resources, transfer_mx, ideal_flops, ga_schedule, wf_name):
        self.name = name
        self.dedicated_resources = dedicated_resources
        self.transfer_mx = transfer_mx
        self.ideal_flops = ideal_flops
        self.ga_schedule = ga_schedule
        self.wf_name = wf_name

class StartEndPair:
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time


















