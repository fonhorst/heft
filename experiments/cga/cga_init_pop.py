from copy import deepcopy
from algs.ga.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE
from experiments.cga.cga_exp import config, do_experiment, _wf, rm, estimator
from experiments.cga.utilities.common import UniqueNameSaver, repeat, extract_initial_pops

cfg = deepcopy(config)
# cfg["generations"] = 3
# path = "../../temp/cga_exp_for_example/5760f488-932e-4224-942f-1f5ac68709bf.json"
# path = "../../temp/good_449.json"
path = "../../temp/bad_560.json"

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
    # saver = UniqueNameSaver("../../temp/cga_init_pop_good")
    saver = UniqueNameSaver("../../temp/cga_init_pop_bad")
    return do_experiment(saver, cfg, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 50)
    print("RESULTS: ")
    print(res)


