from functools import partial

from heft.core.CommonComponents.ExpRunner import run_experiment
from heft.experiments.comparison_experiments.common.ExecutorRunner import ExecutorsFactory


tsk_period = 10
repeat_count = 2
pop_size = 10

def fnc(fl_percent, save_path, wf_name, pop_size):
    #@profile_decorator
    def decoratee(fl_percent):
        res = ExecutorsFactory.default().run_oldpop_executor_with_weakestfailonce(
                                     # for this experiment it doesn't matter at all
                                     reliability=0.95,
                                     is_silent=True,
                                     wf_name=wf_name,
                                     logger=None,
                                     key_for_save='small_run',
                                     # it doesn't matter at all for this experiment
                                     task_id_to_fail="ID00005_000",
                                     fixed_interval_for_ga=15,
                                     ga_params={
                                        "population": pop_size,
                                        "crossover_probability": 0.8,
                                        "replacing_mutation_probability": 0.5,
                                        "sweep_mutation_probability": 0.4,
                                        "generations": 10
                                     },
                                     fail_percent=fl_percent,
                                     save_path=save_path,
                                     check_evolution_for_stopping=False)
        return res
    return decoratee(fl_percent)

def produce_queue(wf_name, tsk_period, repeat_count):
    to_exec = [t for i in range(repeat_count) for t in [0.1, 0.5, 0.8]]
    return to_exec
    pass


if __name__ == "__main__":
    run = partial(run_experiment,
                  fnc=fnc,
                  tsk_period=tsk_period,
                  repeat_count=repeat_count,
                  pop_size=pop_size,
                  produce_queue=produce_queue)
    # run(wf_name="Montage_100")
    run(wf_name="Montage_50")
    pass


