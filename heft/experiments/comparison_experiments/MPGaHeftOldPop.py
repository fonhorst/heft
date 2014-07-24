from functools import partial

from heft.core.CommonComponents.ExpRunner import run_experiment
from heft.experiments.comparison_experiments.common.ExecutorRunner import ExecutorsFactory


tsk_period = 10
repeat_count = 10
pop_size = 50

def fnc(tsk, save_path, wf_name, pop_size):
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
                                         check_evolution_for_stopping=False,
                                         mixed_init_pop=False,
                                         mpnewVSmpoldmode=True)
        return res
    return decoratee(tsk)

if __name__ == "__main__":
    run = partial(run_experiment, fnc=fnc, tsk_period=tsk_period, repeat_count=repeat_count, pop_size=pop_size)
    run(wf_name="Montage_75")
    run(wf_name="Montage_50")
    pass

