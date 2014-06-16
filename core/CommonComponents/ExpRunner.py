#==================================================
# parallel run
#==================================================
from functools import partial
import os
from scoop import futures
from core.comparisons.ComparisonBase import ComparisonUtility


def produce_queue_of_tasks(wf_name, tsk_period, repeat_count):
    tnum = int(wf_name.split("_")[1])
    tasks_to_fail = ["ID000{0}_000".format("0"+str(t) if t < 10 else str(t)) for t in range(0, tnum, tsk_period)]
    to_exec = [t for i in range(repeat_count) for t in tasks_to_fail]
    return to_exec

def run_experiment(fnc, wf_name, tsk_period, repeat_count, pop_size, produce_queue=produce_queue_of_tasks):
    save_path = "../../results/[{0}]_[{1}]_[{2}by{3}]_[{4}]/".format(wf_name, pop_size, tsk_period, repeat_count, ComparisonUtility.cur_time())

    if not os.path.exists(save_path):
        print("create DIR: " + str(save_path))
        os.makedirs(save_path)

    with open(save_path + "timestamp.txt", "w") as f:
        f.write("Start: {0}".format(ComparisonUtility.cur_time()))

    ## TODO: replace it with normal ticket description
    fun = partial(fnc, save_path=save_path, wf_name=wf_name, pop_size=pop_size)
    to_exec = produce_queue(wf_name, tsk_period, repeat_count)
    # res = list(futures.map_as_completed(fun, to_exec))
    res = list(map(fun, to_exec))


    with open(save_path + "timestamp.txt", "a") as f:
        f.write("End: {0}".format(ComparisonUtility.cur_time()))
    pass
