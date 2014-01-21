import xml.etree.ElementTree as ET
from environment.Resource import Task
from environment.Resource import File
from environment.Resource import Workflow

class DAXParser:
    def __init__(self):
        pass

    def readFiles(self, job, task):
        files = job.findall('./{http://pegasus.isi.edu/schema/DAX}uses')
        def buildFile(file):
            return File(file.attrib['file'],int(file.attrib['size']))
        output_files = {fl.name:fl for fl in [buildFile(file) for file in files if file.attrib['link'] == "output"]}
        input_files = {fl.name:fl for fl in [buildFile(file) for file in files if file.attrib['link'] == "input"]}
        task.output_files = output_files
        task.input_files = input_files


    def parseXml(self, filepath, wfId, taskPostfixId):
        tree = ET.parse(filepath)
        root = tree.getroot()
        jobs = root.findall('./{http://pegasus.isi.edu/schema/DAX}job')
        children = root.findall('./{http://pegasus.isi.edu/schema/DAX}child')
        internal_id2Task = dict()
        for job in jobs:
            ## build task
            internal_id = job.attrib['id']
            id = internal_id + "_" + taskPostfixId
            soft = job.attrib['name']
            task = Task(id,internal_id)
            task.soft_reqs.add(soft)
            task.runtime = float(job.attrib['runtime'])
            self.readFiles(job, task)
            internal_id2Task[task.internal_wf_id] = task

        for child in children:
            id = child.attrib['ref']
            parents = [internal_id2Task[prt.attrib['ref']] for prt in child.findall('./{http://pegasus.isi.edu/schema/DAX}parent')]
            child = internal_id2Task[id]
            child.parents.update(parents)
            for parent in parents:
                parent.children.add(child)

        heads = [task for (name,task) in internal_id2Task.items() if len(task.parents) == 0 ]

        common_head = Task("000_" + taskPostfixId, "000")
        for head in heads:
            head.parents = set([common_head])
        common_head.children = heads

        wf = Workflow(wfId, common_head)
        return wf









