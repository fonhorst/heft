from copy import deepcopy
from functools import partial
import os
import random

from heft.algs.common.NewSchedulerBuilder import NewScheduleBuilder
from heft.algs.common.individuals import DictBasedIndividual
from heft.algs.common.particle_operations import CompoundParticle
from heft.algs.ga.GAImplementation.GAFunctions2 import unmoveable_tasks
from heft.algs.pso.ordering_operators import numseq_to_ordering
from heft.core.environment.BaseElements import Node
from heft.experiments.cga.utilities.common import UniqueNameSaver, multi_repeat
from heft.settings import TEMP_PATH


EXAMPLE_BASE_PARAMS = {
    "experiment_name": None,
    "init_sched_percent": 0.05,
    "alg_name": "ga",
    "alg_params": {
        "kbest": 5,
        "n": 10,
        "cxpb": 0.3,  # 0.8
        "mutpb": 0.1,  # 0.5
        "sweepmutpb": 0.3,  # 0.4
        "gen_curr": 0,
        "gen_step": 30,
        "is_silent": True
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


class ParticleScheduleBuilder(NewScheduleBuilder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._unmoveable_tasks = unmoveable_tasks(self.fixed_schedule_part)
        pass


    def _particle_to_chromo(self, particle):
        """
        Converts Particle representation of individual to chromosome representation used by GA operators
        """
        if isinstance(particle, CompoundParticle):
            ordering = numseq_to_ordering(self.workflow, particle.ordering, self._unmoveable_tasks)
            chromo_mapping = {node.name: [] for node in self.nodes}
            for task_id in ordering:
                node_name = particle.mapping.entity[task_id]
                chromo_mapping[node_name].append(task_id)
                pass
            chromo = DictBasedIndividual(chromo_mapping)
            return chromo
        raise ValueError("particle has a wrong type: {0}".format(type(particle)))


    def __call__(self, particle, current_time):

        #TODO: only for debug. remove it later.
        backup_copy = deepcopy(particle)
        alive = [node.name for node in self.nodes if node.state != Node.Down]
        down_nodes = [node.name for node in self.nodes if node.state == Node.Down]
        #print("particle_builder Down_nodes: {0}".format(down_nodes))
        for task_id, node_name in particle.mapping.entity.items():
            if node_name not in alive:
                raise Exception("Exception!")

        chromo = self._particle_to_chromo(particle)
        result = super().__call__(chromo, current_time)
        return result

    pass


def test_run(exp, base_params):
    configs = []
    # reliability = [1.0, 0.95, 0.9]
    # reliability = [1.0]
    reliability = [0.95]
    wf_name = "Montage_25"

    for r in reliability:
        params = deepcopy(base_params)
        params["estimator_settings"]["reliability"] = r
        configs.append(params)

    to_run = [partial(exp, wf_name=wf_name, **params) for params in configs]
    results = [t() for t in to_run]
    # results = multi_repeat(REPEAT_COUNT, to_run)

    saver = UniqueNameSaver(os.path.join(TEMP_PATH, "gaheft_series"), base_params["experiment_name"])
    for result in results:
        saver(result)
    pass


def changing_reliability_run(exp, reliability, repeat_count, wf_names, base_params, save_path=None):
    configs = []
    for r in reliability:
        params = deepcopy(base_params)
        params["estimator_settings"]["reliability"] = r
        configs.append(params)

    to_run = [partial(exp, wf_name=wf_name, **params) for wf_name in wf_names for params in configs]
    results = multi_repeat(repeat_count, to_run)

    path = save_path if save_path is not None else os.path.join(TEMP_PATH, "gaheft_series")
    saver = UniqueNameSaver(path, base_params["experiment_name"])
    for result in results:
        saver(result)
    pass


def inherited_pop_run(exp, wf_tasksids_mapping, repeat_count, base_params, save_path=None):
    to_run = []
    for wf_name, ids in wf_tasksids_mapping.items():
        for id in ids:
            params = deepcopy(base_params)
            params["executor_params"]["task_id_to_fail"] = id
            func = partial(exp, wf_name=wf_name, **params)
            to_run.append(func)

    results = [t() for t in to_run for _ in range(repeat_count)]
    # results = multi_repeat(repeat_count, to_run)

    path = save_path if save_path is not None else os.path.join(TEMP_PATH, "igaheft_series")
    saver = UniqueNameSaver(path, base_params["experiment_name"])
    for result in results:
        saver(result)
    pass


def emigrant_selection(pop, k):
    size = len(pop)
    if k > size:
        raise Exception("Count of emigrants is greater than population: {0}>{1}".format(k, size))
    res = []
    for i in range(k):
        r = random.randint(0, size - 1)
        while r in res:
            r = random.randint(0, size - 1)
        res.append(r)
    return [pop[r] for r in res]
