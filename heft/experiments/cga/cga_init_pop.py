from copy import deepcopy

from heft.algs.ga.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE
from heft.experiments.cga.cga_exp import config, do_experiment, _wf, rm, estimator
from heft.experiments.cga.utilities.common import UniqueNameSaver, repeat, extract_initial_pops
from heft.settings import __root_path__


cfg = deepcopy(config)
path = "{0}/temp/bad_560.json".format(__root_path__)

mps, os = extract_initial_pops(path)

for s in config["species"]:
    if s.name == MAPPING_SPECIE:
        s.initialize = lambda ctx, size: mps
    elif s.name == ORDERING_SPECIE:
        s.initialize = lambda ctx, size: os
    else:
        raise Exception("Unexpected specie: " + s.name)
    pass

def do_exp():
    saver = UniqueNameSaver("{0}/temp/cga_init_pop_bad".format(__root_path__))
    return do_experiment(saver, cfg, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)


