from copy import deepcopy
from functools import partial
from pprint import pprint
from deap.base import Toolbox
import functools
from heft.algs.pso.vm_cpso.cpso import vm_run_cpso, Env
from heft.core.environment import Utility
from heft.core.environment.BaseElements import Node
from heft.core.environment.ResourceManager import Schedule
from heft.experiments.comparison_experiments.gaheft_series.algorithms import create_old_ga
from heft.experiments.comparison_experiments.gaheft_series.experiments import do_cpso
from heft.experiments.comparison_experiments.gaheft_series.utilities import changing_reliability_run, test_run
from heft.algs.pso.vm_cpso.mapordschedule import build_schedule


EXPERIMENT_NAME = "cpso"

REPEAT_COUNT = 30
# WF_NAMES = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
WF_NAMES = ["Montage_75"]
# RELIABILITY = [0.99, 0.975, 0.95, 0.925, 0.9]
RELIABILITY = [0.975]
INDIVIDUALS_COUNTS = [100]

BASE_PARAMS = {
    "experiment_name": EXPERIMENT_NAME,
    "init_sched_percent": 0.05,
    "alg_name": "cpso",

    "alg_params": {
            "pop_size": 50,
            "hall_of_fame_size": 5,
            "hall_idx_change_chance": 0.1,
            "leader_list_size": 5,
            "gamble_size": 100,
            "generations": 100,
            "w": 0.5,
            "c1": 1.6,
            "c2": 1.6,
    },
    "executor_params": {
        "base_fail_duration": 40,
        "base_fail_dispersion": 1,
        "fixed_interval_for_ga": 15,
        "fail_count_upper_limit": 15,
        "replace_anyway": True
    },
    "resource_set": {
        "nodes_conf": [(10, 15, 25, 30)],
        "rules_list": [(80, 30)]
    },
    "estimator_settings": {
        "ideal_flops": 20,
        "transfer_nodes": 100,
        "reliability": 1.0,
        "transfer_blades": 100
    }
}

class CpsoVmWrapper:
    def __init__(self, _wf, rm, estimator,
                     init_sched_percent,
                     log_book, stats,
                     alg_params):
        self._wf = _wf
        self.rm = rm
        self.estimator = estimator
        self.init_sched_percent = init_sched_percent
        self.log_book = log_book
        self.stats = stats
        self.alg_params = alg_params
        pass

    def __call__(self, fixed_schedule_part, initial_schedule, current_time=0, initial_population=None):

        kwargs = deepcopy(self.alg_params)
        kwargs["env"] = Env(self._wf, self.rm, self.estimator)
        kwargs["fixed_schedule"] = fixed_schedule_part
        kwargs["initial_schedule"] = initial_schedule
        kwargs["current_time"] = current_time
        kwargs["initial_population"] = initial_population
        print("CPSO_START")
        best, pops, logbook, initial_pops, hall, vm_series = vm_run_cpso(**kwargs)
        print("RES config = " + str([node.flops for node in best[0][1].entity.get_live_nodes()]))
        ms = [(t, n) for t, n in best[0][0].mapping.entity.items()]
        temp_os = [(k, v) for k, v in best[0][0].ordering.entity.items()]
        temp_os.sort(key=lambda x: x[1])
        os = [t[0] for t in temp_os]
        solution = dict()
        solution['MappingSpecie'] = ms
        solution['OrderingSpecie'] = os
        schedule = build_schedule(self._wf, self.estimator, self.rm, fixed_schedule_part, current_time, solution, best[0][1])

        if any( not isinstance(node, Node) for node in schedule.mapping):
            print("Node types: ", [type(node) for node in schedule.mapping])
            raise Exception("Alarm! a node in built schedule has incorrect type")
        ## TODO: this is a hack for correct algorithm work. It should be removed later
        # correct_schedule = Schedule({rm.node(node_name): items for node_name, items in schedule.mapping.items()})
        correct_schedule = schedule
        schedule_nodes = set(correct_schedule.mapping.keys())
        if len(schedule_nodes.symmetric_difference(self.rm.get_nodes())) > 0:
            print("Rm_nodes", self.rm.get_nodes())
            print("Schedule nodes", schedule_nodes)
            raise Exception("Alarm! The new schedule doesn't contain all possible nodes from ResourceManager")
        #pprint(correct_schedule.mapping)
        Utility.Utility.validate_is_schedule_complete(self._wf, correct_schedule)
        #Utility.Utility.validate_static_schedule(_wf, correct_schedule)
        if None in correct_schedule.mapping:
            raise Exception("Invalid name of node. Perhaprs resource manager in inconsistent state")
        # logbook = None
        # print("CGA_STOP")
        #pprint(correct_schedule.mapping)
        return (best, pops, correct_schedule, None), logbook
        # TODO: debug. Just for test
        #return (None, None, initial_schedule, None), logbook

def create_cpso(_wf, rm, estimator,
                     init_sched_percent,
                     log_book, stats,
                     alg_params):

    return CpsoVmWrapper(_wf, rm, estimator,
                        init_sched_percent,
                        log_book, stats,
                        alg_params)



cpso_exp = partial(do_cpso, alg_builder=create_cpso)

if __name__ == "__main__":
    p = deepcopy(BASE_PARAMS)
    # test_run(ga_exp, BASE_PARAMS)
    changing_reliability_run(cpso_exp, RELIABILITY, INDIVIDUALS_COUNTS, REPEAT_COUNT, WF_NAMES, BASE_PARAMS)
