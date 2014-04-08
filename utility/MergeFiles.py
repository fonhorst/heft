import json
from os import listdir
import os
from os.path import isfile, join

dir = '../results/nresult/'
key = 'small_run'
united_file_name = "united.json"

files = [f for f in listdir(dir) if isfile(join(dir, f)) and f.endswith(".json") and key in f]

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
    os.remove(join(dir, f))



