from functools import partial
from deap.base import Toolbox
from heft.algs.common.common_fixed_schedule_schema import run_ga


def create_pfga():
    toolbox = Toolbox()
    toolbox.register("",)
    alg = partial(run_ga, toolbox=toolbox)
    return alg
