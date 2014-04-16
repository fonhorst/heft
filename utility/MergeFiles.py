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

    dir = "D:/wspace/heft/results/m50_gaheft_oldpop_10by10_selbest/"
    # dir = "D:/wspace/heft/results/"
    # dir = "D:/wspace/heft/results/m_[50x3]/tournament/"
    pathes = generate_pathes(dir, key)

    fnc = partial(mergefiles, key=key)
    list(futures.map_as_completed(fnc, pathes))




