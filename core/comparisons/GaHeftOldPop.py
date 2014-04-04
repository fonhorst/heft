from functools import partial
from core.runners.ExecutorRunner import ExecutorsFactory

func = partial(ExecutorsFactory.default().run_gaheftoldpop_executor,
                                     reliability=0.95,
                                     is_silent=True,
                                     wf_name="Montage_50",
                                     logger=None,
                                     key_for_save='small_run',
                                     #task_id_to_fail="ID00005_000",
                                     fixed_interval_for_ga=15,
                                     ga_params={
                                        "population": 50,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 10
                                     })

tasks_to_fail = ["ID00005_000"]
# tasks_to_fail = ["ID00010_000"]
# tasks_to_fail = ["ID00015_000"]
# tasks_to_fail = ["ID00020_000"]
# tasks_to_fail = ["ID00025_000"]
# tasks_to_fail = ["ID00030_000"]
# tasks_to_fail = ["ID00035_000"]
# tasks_to_fail = ["ID00040_000"]
# tasks_to_fail = ["ID00045_000"]
# tasks_to_fail = ["ID00049_000"]

#==================================================
# small run
#==================================================
# for t in tasks_to_fail:
#     for i in range(3):
#         func(task_id_to_fail=t)
#         pass
#     pass

#==================================================
# medium run
#==================================================
for t in tasks_to_fail:
    for i in range(25):
        func(task_id_to_fail=t)
        pass
    pass

