import os
from uuid import uuid4
from heft.algs.common.algorithm_factory import create_pfga
from heft.algs.heft.DSimpleHeft import DynamicHeft
from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator

from heft.experiments.comparison_experiments.common.ComparisonBase import ResultSaver, ComparisonUtility
from heft.experiments.comparison_experiments.common.VersusFunctors import GaHeftvsHeft
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.experiments.comparison_experiments.executors.GaHeftExecutor import GaHeftExecutor

REPEAT_COUNT = 1
wf_name = "Montage_25"

BASE_PARAMS = {
    "init_sched_percent": 0.05,
    "alg_name": "ga",
    "alg_params": {
        "kbest": 5,
        "n": 100,
        "cxpb": 0.3,  # 0.8
        "mutpb": 0.1,  # 0.5
        "sweepmutpb": 0.3,  # 0.4
        "gen_curr": 0,
        "gen_step": 300
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15
    },
    "resource_set": {
        "nodes_conf": [10, 15, 25, 30],
    },
    "estimator_settings": {
        "transferMx": None,
        "comp_time_cost": 0,
        "transf_time_cost": 0,
        "ideal_flops": 20,
        "transfer_time": 100,
        "reliability": 1.0
    }
}

def do_exp(wf_name, **params):
    _wf = wf("Montage_100")
    rm = ExperimentResourceManager(rg.r(params["resource_set"]))
    estimator = SimpleTimeCostEstimator(params["estimator_settings"])
    dynamic_heft = DynamicHeft(_wf, rm, estimator)
    ga = create_pfga(_wf, rm, estimator,
                     params["init_sched_percent"],
                     logbook=None, stats=None,
                     **params["ga_params"])
    machine = GaHeftExecutor(heft_planner=dynamic_heft,
                             wf=_wf,
                             resource_manager=rm,
                             ga_builder=lambda: ga,
                             **params["executor_params"])

    machine.init()
    machine.run()
    resulted_schedule = machine.current_schedule

    data = {
        "wf_name": wf_name,
        "params": params,
        "result": {
            "makespan": Utility.makespan(resulted_schedule),
            "overall_transfer_time": Utility.overall_transfer_time(resulted_schedule, _wf, ESTIMATOR),
            "overall_execution_time": Utility.overall_execution_time(resulted_schedule)
        }
    }

    return data






save_file_name = ComparisonUtility.build_save_path(wf_name + '\\GaHeftvsHeft_['+str(uuid4())+']')
result_saver = ResultSaver(save_file_name)
exp = GaHeftvsHeft(reliability, n=1)
def calc(wf_name, out):
    return result_saver(exp(wf_name, out))

print("fail_duration: 40")
print("reliability %s" % reliability)

base_dir = "../../resources/experiment_1/"
if not os.path.exists(base_dir):
    os.makedirs(base_dir)
output_file_template = base_dir + "[{0}]_[{1}]_[{2}].txt"
out = lambda w_name: output_file_template.format(w_name, reliability, ComparisonUtility.cur_time())

wf_names = [wf_name]

[calc(wf_name, out(wf_name)) for wf_name in wf_names]
