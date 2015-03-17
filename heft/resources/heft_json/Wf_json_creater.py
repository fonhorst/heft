import json
from heft.algs.heft.DSimpleHeft import run_heft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.settings import __root_path__
import os
from heft.core.environment.Utility import wf

def schedule_to_json(schedule, file_path):
    """
    Schedule to JSON file
    """
    start_time_list = []
    mapping_list = []
    for item in schedule.mapping:
        for task in schedule.mapping[item]:
            mapping_list.append([task.job.id, item.flops])
            start_time_list.append((task.job.id, task.start_time))
    start_time_list.sort(key=lambda x: x[1])
    res = json.dumps({"mapping": mapping_list, "ordering": [task for task, time in start_time_list]})
    file = open(file_path, 'w')
    file.write(res)
    file.close()

if __name__ == "__main__":
    wf_list = ["Montage_25", "Montage_30", "Montage_50", "Montage_75", "Montage_100", "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100"]
    rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
    estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)
    for wf_name in wf_list:
        _wf = wf(wf_name)
        schedule = run_heft(_wf, rm, estimator)
        json_path = os.path.join(__root_path__, "heft\\resources\\heft_json\\", "heft_" + wf_name.lower() + ".json")
        schedule_to_json(schedule, json_path)

