from functools import partial
import os
from scoop import futures
from core.comparisons.ComparisonBase import ComparisonUtility
from core.runners.ExecutorRunner import ExecutorsFactory
from environment.Utility import profile_decorator

default_wf_name = "Montage_100"
tsk_period = 10
repeat_count = 10
pop_size = 30
default_save_path = "../../results/[{0}]_[{1}]_[{2}by{3}]/".format(default_wf_name, pop_size, tsk_period, repeat_count)

def fnc(tsk, save_path, wf_name=default_wf_name):
    #@profile_decorator
    def decoratee(tsk):
        res = ExecutorsFactory.default().run_mpgaheftoldpop_executor(
                                         # for this experiment it doesn't matter at all
                                         reliability=0.95,
                                         is_silent=True,
                                         wf_name=wf_name,
                                         logger=None,
                                         key_for_save='small_run',
                                         #task_id_to_fail="ID00005_000",
                                         task_id_to_fail=tsk,
                                         fixed_interval_for_ga=15,
                                         migrCount=5,
                                         emigrant_selection=None,
                                         all_iters_count=300,
                                         merged_pop_iters=100,
                                         ga_params={
                                            "population": pop_size,
                                            "crossover_probability": 0.8,
                                            "replacing_mutation_probability": 0.5,
                                            "sweep_mutation_probability": 0.4,
                                            "generations": 10
                                         },
                                         save_path=save_path,
                                         mixed_init_pop=False,
                                         mpnewVSmpoldmode=True)
        return res
    return decoratee(tsk)

#==================================================
# parallel run
#==================================================

def produce_queue_of_tasks(wf_name):
    tnum = int(wf_name.split("_")[1])
    tasks_to_fail = ["ID000{0}_000".format("0"+str(t) if t < 10 else str(t)) for t in range(0, tnum, tsk_period)]
    to_exec = [t for i in range(repeat_count) for t in tasks_to_fail]
    return to_exec

def run_experiment(wf_name):
    save_path = "../../results/[{0}]_[{1}]_[{2}by{3}]_[{4}]/".format(wf_name, pop_size, tsk_period, repeat_count, ComparisonUtility.cur_time())

    if not os.path.exists(save_path):
        print("create DIR: " + str(save_path))
        os.makedirs(save_path)

    with open(save_path + "timestamp.txt", "w") as f:
        f.write("Start: {0}".format(ComparisonUtility.cur_time()))

    ## TODO: replace it with normal ticket description
    fun = partial(fnc, save_path=save_path, wf_name=wf_name)
    to_exec = produce_queue_of_tasks(wf_name)
    res = list(futures.map_as_completed(fun, to_exec))


    with open(save_path + "timestamp.txt", "a") as f:
        f.write("End: {0}".format(ComparisonUtility.cur_time()))

if __name__ == "__main__":
    run_experiment(wf_name="Montage_75")
    run_experiment(wf_name="Montage_50")
    pass

