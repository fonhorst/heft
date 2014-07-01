from functools import partial
import functools
import json
from os import listdir
import os
from os.path import isfile, join

# dir = '../results/m_[30x3]/m75_[30x3]_10by10_tour4/'
from scoop import futures

key = 'small_run'

def mergefiles(dir, key):
    print("Proccessing dir {0}...".format(dir))
    files = [f for f in listdir(dir) if isfile(join(dir, f)) and f.endswith(".json") and key in f]

    if len(files) == 0:
        return

    def l(file):
        with open(dir + file, 'r') as f:
            result_array = json.load(f)
        return result_array

    result = []
    for f in files:
        result = result + l(f)

    with open(dir + key + ".json", 'w') as f:
        json.dump(result, f)

    for f in files:
        if key + ".json" not in f:
            os.remove(join(dir, f))
    pass

if __name__ == "__main__":
    # dir = '../results/m_[30x3]/m75_[30x3]_10by10_tour4/'
    # mergefiles(dir, key)

    def generate_pathes(dir, key):
        seq = (generate_pathes(dir + entry + "/", key) for entry in os.listdir(dir) if os.path.isdir(dir + entry))
        result = functools.reduce(lambda x, y: x + y, seq, [dir])
        return result

    # dir = "D:/wspace/heft/results/new_experiments_for_ECTA/sw2/additional_strongest/"
    # dir = "D:/wspace/heft/results/m250/"
    # dir = "D:/wspace/heft/results/[Montage_250]_[50]_[10by20]_[18_06_14_17_52_26]/"
    # dir = "D:/wspace/heft/results/[Montage_250]_[50]_[10by20]_[18_06_14_18_16_37]/"
    # dir = "D:/wspace/heft/results/[Montage_250]_[50]_[10by5]_[18_06_14_18_41_42]/"
    # dir = "D:/wspace/heft/results/m250_[120-180]/"
    # dir = "D:/wspace/heft/results/[Montage_250]_[50]_[10by20]_[18_06_14_19_09_24]/"
    # dir = "D:/wspace/heft/results/[Montage_250]_[50]_[10by5]_[19_06_14_10_43_15]/"
    dir = "D:/wspace/heft/results/[Montage_100]_[50]_[10by1]_[20_06_14_13_13_51]/"
    # dir = "D:/wspace/heft/results/"
    # dir = "D:/wspace/heft/results/m_[50x3]/tournament/"
    pathes = generate_pathes(dir, key)

    fnc = partial(mergefiles, key=key)
    list(futures.map_as_completed(fnc, pathes))




