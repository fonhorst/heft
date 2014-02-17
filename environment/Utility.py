import json
import os
import subprocess
from core.comparisons.ComparisonBase import ComparisonUtility
from environment.DAXParser import DAXParser
from random import Random
from environment.Resource import Node, Resource
from environment.ResourceManager import ScheduleItem, Schedule
import xml.etree.ElementTree as ET


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
        wfs = [parser.parseXml(dax_filepath, wf_start_id + str(i), task_postfix_id + str(i)) for i in
               range(0, pipelineSize)]
        for wf in wfs:
            wf.deadline = deadline
        return wfs

    @staticmethod
    def readWorkflow(dax_filepath, wf_start_id="00", task_postfix_id="00", deadline=1000):
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

        task_to_node = {job_id: sorted(seq, key=lambda x: x.start_time) for (job_id, seq) in task_to_node.items()}
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
                if next is None and pred.end_time <= pair.start_time:
                    return True
                if pred.end_time <= pair.start_time and next.start_time >= pair.end_time:
                    return True

                return False

            reslt = False in [check(pair) for pair in unavil_seq]
            if reslt is True:
                return False
            return True

        reslt = False in [check_unavailability(schedule.mapping[node], seq) for (node, seq) in
                          unavailability_periods.items()]
        if reslt is True:
            return False
        return True

    @staticmethod
    def get_the_last_time(schedule):
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

                dct['ga_schedule'].mapping = {all_nodes[node_name]: values for (node_name, values) in
                                              dct['ga_schedule'].mapping.items()}

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

        name = wf_name + "_bundle"
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

    @staticmethod
    def check_fixed_part(schedule, fixed_part, current_time):
        def item_equality(item1, fix_item):

            is_equal = item1.state == fix_item.state
            not_finished = (fix_item.state == ScheduleItem.UNSTARTED or fix_item.state == ScheduleItem.EXECUTING)
            is_finished_now = (
                not_finished and item1.state == ScheduleItem.FINISHED and fix_item.end_time <= current_time)
            is_executing_now = (
                not_finished and item1.state == ScheduleItem.EXECUTING and fix_item.start_time <= current_time <= fix_item.end_time )
            is_state_correct = is_equal or is_finished_now or is_executing_now

            return item1.job.id == fix_item.job.id and is_state_correct and item1.start_time == fix_item.start_time and item1.end_time == fix_item.end_time

        for (node, items) in fixed_part.mapping.items():
            #TODO: need to make here search by node.name
            itms = schedule.mapping[node]
            for i in range(len(items)):
                if not item_equality(itms[i], items[i]):
                    return False
        return True

    @staticmethod
    def check_duplicated_tasks(schedule):
        task_instances = dict()
        for (node, items) in schedule.mapping.items():
            for item in items:
                instances = task_instances.get(item.job.id, [])
                instances.append((node, item))
                task_instances[item.job.id] = instances

        for (id, items) in task_instances.items():
            sts = [item.state for (node, item) in items]
            inter_excluded_states = list(filter(
                lambda x: x == ScheduleItem.FINISHED or x == ScheduleItem.EXECUTING or x == ScheduleItem.UNSTARTED,
                sts))
            if len(inter_excluded_states) > 1:
                return False
            pass
        return True

    @staticmethod
    def schedule_to_jed(schedule):

        cmap_tmpl = '<cmap name="default"><conf name="min_font_size_label" value="10" /><conf name="font_size_label" value="18" /><conf name="font_size_axes" value="18" /><task id="wf1"><color type="fg" rgb="FFFFFF" /><color type="bg" rgb="0000FF" /></task></cmap>'

        cmap = ET.fromstring(cmap_tmpl)
        nodes_count = len(schedule.mapping.keys())

        grid_schedule = ET.Element('grid_schedule')
        meta_info = ET.fromstring('<meta_info>\
		                <meta name="alloc" value="mcpa"/>\
		                <meta name="pack" value="0"/>\
		                <meta name="bf" value="0"/>\
		                <meta name="ialloc" value="0"/>\
	                   </meta_info>')

        grid_info = ET.fromstring('<grid_info>\
		    <info name="nb_clusters" value="1"/>\
		    <clusters>\
		        <cluster id="0" hosts="{0}" first_host="0"/>\
		    </clusters>\
	    </grid_info>'.format(nodes_count))

        task_tmpl = '<node_statistics>\
			<node_property name="id" value="{0}"/>\
			<node_property name="type" value="wf1"/>\
			<node_property name="start_time" value="{1}"/>\
			<node_property name="end_time" value="{2}"/>\
			<configuration>\
			  <conf_property name="cluster_id" value="0"/>\
			  <conf_property name="host_nb" value="1"/>\
			  <host_lists>\
			    <hosts start="{3}" nb="1"/>\
			  </host_lists>\
			</configuration>\
		</node_statistics>'

        grid_schedule.append(meta_info)
        grid_schedule.append(grid_info)
        node_infos = ET.SubElement(grid_schedule, 'node_infos')

        keys = schedule.mapping.keys()
        # node to sequence number
        ns = {node.name: i for (node, i) in zip(keys, range(len(keys)))}

        for (node, items) in schedule.mapping.items():
            for item in items:
                el = ET.fromstring(task_tmpl.format(item.job.id, item.start_time, item.end_time, ns[node.name]))
                node_infos.append(el)

        return ET.ElementTree(grid_schedule), ET.ElementTree(cmap), ns

    @staticmethod
    def write_schedule_to_jed(schedule, jed_path, cmap_path, node_mapping_path):
        def write_node_mapping(f, mapping):
            for (name, i) in mapping.items():
                f.write('{0} <- {1}\n'.format(i, name))
            pass

        (grid_schedule, cmap, ns) = Utility.schedule_to_jed(schedule)
        jed_file = open(jed_path, 'wb')
        cmap_file = open(cmap_path, 'wb')
        node_mapping_file = open(node_mapping_path, 'w')
        grid_schedule.write(jed_file)
        cmap.write(cmap_file)
        write_node_mapping(node_mapping_file, ns)
        jed_file.close()
        cmap_file.close()
        node_mapping_file.close()
        pass

    @staticmethod
    def create_jedule_visualization(schedule, name):
        ## TODO: fix it later.
        old_dir = os.getcwd()
        os.chdir('../../')

        folder = './resources/schedule_visualization/' + name + '_' + ComparisonUtility.cur_time()
        jed_path = folder + '/' + name + '.jed'
        cmap_path = folder + '/' + 'cmap.xml'
        node_mapping_path = folder + '/' + 'node_mapping.txt'
        output_path = folder + '/' + 'output.png'

        os.makedirs(folder)

        Utility.write_schedule_to_jed(schedule, jed_path, cmap_path, node_mapping_path)

        p = subprocess.Popen('java -Xmx512M -jar ./jedule-0.3.2.jar net.sf.jedule.JeduleStarter \
                            -f {0} -p simgrid -o {1} -d 1024x768 \
                            -cm {2}'.format(jed_path, output_path, cmap_path))
        p.communicate()

        ## TODO: fix it later.
        os.chdir(old_dir)
        pass

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


















