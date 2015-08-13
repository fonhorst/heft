import cProfile
import functools
import json
import operator
import os
import pstats
import subprocess
import io
import time
from random import Random
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import matplotlib.image as mpimg




class Vizualizator:
    def __init__(self):
        pass

    @staticmethod
    def schedule_to_jed(schedule):

        cmap_tmpl = '<cmap name="default">' \
                    '<conf name="min_font_size_label" value="10" />' \
                    '<conf name="font_size_label" value="18" />' \
                    '<conf name="font_size_axes" value="18" />' \
                    '<task id="wf1"><color type="fg" rgb="FFFFFF" />' \
                    '<color type="bg" rgb="0000FF" /></task>' \
                    '<task id="wf2"><color type="fg" rgb="FFFFFF" />' \
                    '<color type="bg" rgb="00FF00" /></task>' \
                    '<task id="wf3"><color type="fg" rgb="FFFFFF" />' \
                    '<color type="bg" rgb="FF0000" /></task>' \
                    '</cmap>'

        cmap = ET.fromstring(cmap_tmpl)
        nodes_count = len(schedule.mapping.keys())

        grid_schedule = ET.Element('grid_schedule')
        meta_info = ET.fromstring('<meta_info> \
		            <meta name="alloc" value="mcpa"/> \
		            <meta name="pack" value="0"/> \
		            <meta name="bf" value="0"/> \
		                <meta name="ialloc" value="0"/> \
	                   </meta_info>')

        grid_info = ET.fromstring('<grid_info> \
		    <info name="nb_clusters" value="1"/> \
		    <clusters> \
		        <cluster id="0" hosts="{0}" first_host="0"/> \
		    </clusters> \
	        </grid_info>'.format(nodes_count))

        task_tmpl = '<node_statistics> \
			<node_property name="id" value="{0}"/> \
			<node_property name="type" value="{4}"/> \
			<node_property name="start_time" value="{1}"/> \
			<node_property name="end_time" value="{2}"/> \
			<configuration> \
			  <conf_property name="cluster_id" value="0"/> \
			  <conf_property name="host_nb" value="1"/> \
			  <host_lists> \
			    <hosts start="{3}" nb="1"/> \
			  </host_lists> \
			</configuration> \
		    </node_statistics>'

        wf_map = []
        for val in schedule.mapping.values():
            for sched in val:
                if sched.job.wf not in wf_map:
                    wf_map.append(sched.job.wf)

        grid_schedule.append(meta_info)
        grid_schedule.append(grid_info)
        node_infos = ET.SubElement(grid_schedule, 'node_infos')

        keys = schedule.mapping.keys()
        # node to sequence number
        ns = {node: i for (node, i) in zip(keys, range(len(keys)))}

        for (node, items) in schedule.mapping.items():
            for item in items:
                el = ET.fromstring(task_tmpl.format(item.job.id, item.start_time, item.end_time, ns[node],
                                                    "wf" + str(wf_map.index(item.job.wf) + 1)))
                node_infos.append(el)

        return ET.ElementTree(grid_schedule), ET.ElementTree(cmap), ns

    @staticmethod
    def write_schedule_to_jed(schedule, jed_path, cmap_path, node_mapping_path):
        def write_node_mapping(f, mapping):
            for (node, i) in sorted(mapping.items(), key=lambda x: x[1]):
                f.write('{0} <- name: {1}; flops: {2} \n'.format(i, node.name, node.flops))
            pass

        (grid_schedule, cmap, ns) = Vizualizator.schedule_to_jed(schedule)
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
    def create_jedule_visualization(schedule, folder, name):

        jed_path = folder + '\jed\\' + name + '.jed'
        cmap_path = folder + '\cmap\\' + 'cmap.xml'
        node_mapping_path = folder + '\mapping\\' + 'node_mapping.txt'
        output_path = folder + '\\res\\' + name + '.png'

        # if not os.path.exists(folder):
        #     os.makedirs(folder)
        # if not os.path.exists(jed_path):
        #     os.makedirs(jed_path)
        # if not os.path.exists(cmap_path):
        #     os.makedirs(cmap_path)
        # if not os.path.exists(node_mapping_path):
        #     os.makedirs(node_mapping_path)
        # if not os.path.exists(output_path):
        #     os.makedirs(output_path)

        Vizualizator.write_schedule_to_jed(schedule, jed_path, cmap_path, node_mapping_path)

        p = subprocess.Popen(
            ['java', '-Xmx512M', '-jar', 'D:\Projects\heft_simulator\heft\heft\core\\vizualization\jedule-0.3.0.jar',
             'net.sf.jedule.JeduleStarter',
             '-f', jed_path, "-gt", "png", '-p', 'simgrid', '-o', output_path, '-d', '480x320',
             '-cm', cmap_path])
        p.communicate()

        from PIL import Image
        img = Image.open(output_path)
        img.show()
        pass


pass


