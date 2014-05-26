from copy import deepcopy
import json
from GA.DEAPGA.coevolution.cga import ListBasedIndividual
from GA.DEAPGA.coevolution.operators import MAPPING_SPECIE, ORDERING_SPECIE
from experiments.cga.cga_exp import config, do_experiment, _wf, rm, estimator
from experiments.cga.utilities.common import UniqueNameSaver, repeat

cfg = deepcopy(config)
cfg["generations"] = 3
path = "../../temp/cga_exp_for_example/5760f488-932e-4224-942f-1f5ac68709bf.json"

with open(path, "r") as f:
    data = json.load(f)
initial_pops = data["initial_pops"]

mps = [ListBasedIndividual([tuple(g) for g in ind]) for ind in initial_pops[MAPPING_SPECIE]]
os = [ListBasedIndividual(ind) for ind in initial_pops[ORDERING_SPECIE]]

for s in config["species"]:
    if s.name == MAPPING_SPECIE:
        s.initialize = lambda ctx, size: mps
    elif s.name == ORDERING_SPECIE:
        s.initialize = lambda ctx, size: os
    else:
        raise Exception("Unexpected specie: " + s.name)
    pass

def do_exp():
    saver = UniqueNameSaver("../../temp/cga_init_pop")
    return do_experiment(saver, cfg, _wf, rm, estimator)

if __name__ == "__main__":
    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)


