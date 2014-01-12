__author__ = 'Николай'

from environment.DAXParser import DAXParser
from random import Random
from environment.Resource import Workflow
from environment.Resource import Task
import copy

class Utility:

    MIN_PIPELINE_SIZE = 10
    MAX_PIPELINE_SIZE = 40

    def __init__(self):
         pass

    def generateUrgentPipeline(self, dax_filepath, wf_start_id, task_postfix_id, deadline):
        parser = DAXParser()
        random = Random()
        pipelineSize = random.randint(Utility.MIN_PIPELINE_SIZE,Utility.MAX_PIPELINE_SIZE)
        wfs = [parser.parseXml(dax_filepath,wf_start_id + str(i), task_postfix_id + str(i)) for i in range(0, pipelineSize)]
        for wf in wfs:
            wf.deadline = deadline
        return wfs










