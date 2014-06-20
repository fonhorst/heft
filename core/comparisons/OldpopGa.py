from functools import partial
from core.CommonComponents.ExpRunner import run_experiment
from core.runners.ExecutorRunner import ExecutorsFactory


tsk_period = 10
repeat_count = 1

def queue_of_tasks(*args, **kwargs):
    return ["ID00000_000"]

pop_size = 50

def fnc(tsk, save_path, wf_name, pop_size):
    #@profile_decorator
    def decoratee(tsk):
        res = ExecutorsFactory.default().run_oldpop_executor(
                                     # for this experiment it doesn't matter at all
                                     reliability=0.95,
                                     is_silent=True,
                                     wf_name=wf_name,
                                     logger=None,
                                     key_for_save='small_run',
                                     task_id_to_fail=tsk,
                                     fixed_interval_for_ga=15,
                                     ga_params={
                                        "population": pop_size,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 100
                                     },
                                     save_path=save_path,
                                     check_evolution_for_stopping=False,
                                     nodes_conf=[10, 10, 10, 10])
        return res
    return decoratee(tsk)



if __name__ == "__main__":
    run = partial(run_experiment, fnc=fnc, tsk_period=tsk_period, repeat_count=repeat_count, pop_size=pop_size, produce_queue=queue_of_tasks)
    run(wf_name="Montage_100")
    # run(wf_name="Montage_50")
    pass


