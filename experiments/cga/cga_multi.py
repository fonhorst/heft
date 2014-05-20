from copy import deepcopy
from functools import partial
import os
from scoop import futures
from experiments.cga.cga_exp import config, repeat, do_experiment
from experiments.cga.utilities.common import UniqueNameSaver



base_path = "../../temp/cga_partial_experiments/"

cfg_50_interact = deepcopy(config)
cfg_50_interact["interact_individuals_count"] = 50

cfg_100_interact = deepcopy(config)
cfg_100_interact["interact_individuals_count"] = 100

cfg_200_interact = deepcopy(config)
cfg_200_interact["interact_individuals_count"] = 200

cfg_500_interact = deepcopy(config)
cfg_500_interact["interact_individuals_count"] = 500

cfg_20_psize = deepcopy(config)
for specie in cfg_20_psize["species"]:
    specie.pop_size = 20

cfg_50_psize = deepcopy(config)
for specie in cfg_50_psize["species"]:
    specie.pop_size = 50

cfg_100_psize = deepcopy(config)
for specie in cfg_100_psize["species"]:
    specie.pop_size = 100

cfg_250_psize_500_interact = deepcopy(config)
cfg_250_psize_500_interact["interact_individuals_count"] = 500
for specie in cfg_250_psize_500_interact["species"]:
    specie.pop_size = 250


tasks = [(cfg_50_interact, 'cga_exp_50_interact'),
             (cfg_100_interact, 'cga_exp_100_interact'),
             (cfg_200_interact, 'cga_exp_200_interact'),
             (cfg_500_interact, 'cga_exp_500_interact'),
             ## pop_size dependency
             (cfg_20_psize, 'cga_exp_20_pop_size'),
             (cfg_50_psize, 'cga_exp_50_pop_size'),
             (cfg_100_psize, 'cga_exp_100_pop_size'),
             (cfg_250_psize_500_interact, 'cga_exp_250_pop_size')
             ]


def do_exp(arg):
    config, name = tasks[arg]
    saver = UniqueNameSaver(os.path.join(base_path, name))
    return do_experiment(saver, config)

REPEAT_COUNT = 10

if __name__ == "__main__":
    for i in range(REPEAT_COUNT):
        list(futures.map(do_exp, range(len(tasks))))

