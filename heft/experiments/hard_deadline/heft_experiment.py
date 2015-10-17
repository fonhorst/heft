from heft.algs.heft.DeadlineHeft import run_heft
from heft.core.CommonComponents.BladeExperimentalManager import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment.Utility import wf, Utility
from heft.experiments.cga.mobjective.utility import SimpleTimeCostEstimator
from heft.core.environment.ResourceGenerator import ResourceGenerator as rg
from heft.core.environment.BaseElements import Workflow
from heft.core.environment.BaseElements import Task

rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = SimpleTimeCostEstimator(comp_time_cost=0, transf_time_cost=0, transferMx=None,
                                    ideal_flops=20, transfer_time=100)

def do_exp(wf_info):
    common_head = Task("0_", "0", True)
    _work_wf = Workflow("test", "test", common_head)
    wfs = set()
    for i in range(0,len(wf_info)):
        _wf = wf(wf_info[i],is_head=False, task_postfix_id=str(i))
        wfs.update(set([_wf]))

    def set_priority(task, priority):
        task.priority = priority
        for i in range(0, len(task.children)):
            child_task = task.children.pop()
            set_priority(child_task, priority)
            task.children.add(child_task)

    wfs = sorted(wfs, key=lambda wf: wf.deadline != 0)
    wfs = list(reversed(wfs))
    for i in range(0,int(len(wfs))):
        current_wf = wfs.pop()
        for task in current_wf.head_task.children:
            task.parents = set([common_head])
            if current_wf.deadline != 0:
                set_priority(task, i+1)
            else:
                set_priority(task, 0)
        common_head.children.update(set(current_wf.head_task.children))

    heft_schedule = run_heft(_work_wf, rm, estimator)
    Utility.validate_static_schedule(_work_wf, heft_schedule)
    makespan = Utility.makespan(heft_schedule)
    return makespan

if __name__ == "__main__":
    result = do_exp(["DAM"])
    print(result)
