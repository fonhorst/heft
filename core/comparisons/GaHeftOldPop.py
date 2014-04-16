from functools import partial
from scoop import futures
from core.runners.ExecutorRunner import ExecutorsFactory
wf_name = "Montage_50"
tsk_period = 10
repeat_count = 5
save_path = "D:/wspace/heft/results/m50_gaheft_oldpop_20__10by5_2/"
def fnc(tsk):
    return ExecutorsFactory.default().run_gaheftoldpop_executor(
                                     reliability=0.95,
                                     is_silent=True,
                                     wf_name=wf_name,
                                     logger=None,
                                     key_for_save='small_run',
                                     #task_id_to_fail="ID00005_000",
                                     task_id_to_fail=tsk,
                                     fixed_interval_for_ga=15,
                                     save_path=save_path,
                                     ga_params={
                                        "population": 20,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 300
                                     },
                                     check_evolution_for_stopping=False)

#==================================================
# parallel run
#==================================================
tnum = int(wf_name.split("_")[1])
tasks_to_fail = ["ID000{0}_000".format("0"+str(t) if t < 10 else str(t)) for t in range(0, tnum, tsk_period)]
to_exec = [t for i in range(repeat_count) for t in tasks_to_fail]

if __name__ == "__main__":
    res = list(futures.map_as_completed(fnc, to_exec))
    pass