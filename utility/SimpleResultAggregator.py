## simple clustering by count of fails
## input: file_name output: (file_name, file_params, heft_clustering_dict, ga_clustering_dict)
## file_params = (reliability, wf_name, datetime)
## _clustering_dict = {
##      failure_count: [result_value, ... ]
##      ...
## }
import os
import math


def aggregate_fails(file_name):
    f = open(file_name, 'r')
    line = f.readline()

    heft_clustering = dict()
    ga_clustering = dict()
    clustering = None
    current_counter = 0
    current_run_result = None

    while line != '':
        if "=== HEFT Run" in line:
            clustering = heft_clustering
            pass
        if "=== GAHEFT Run" in line:
            clustering = ga_clustering
            pass
        if "Run heft" in line:
            current_counter = 0
            pass
        if "Run gaHeft" in line:
            current_counter = 0
            pass
        if "Event: NodeFailed" in line:
            current_counter += 1
            pass
        if "====RESULT====" in line:
            current_run_result = float(f.readline().strip())
            m = clustering.get(current_counter, [])
            m.append(current_run_result)
            clustering[current_counter] = m
            pass
        line = f.readline()
        pass
    f.close()
    ## TODO: add file_params later
    return (file_name, None,heft_clustering, ga_clustering)
    pass

## TODO: it's a test run. Extend it and make unit test later.
# result = aggregate_fails("D:/testdir/_GA_VS_HEFT/[Montage_25]_[0.95]_[20_02_14_16_34_39].txt")


## input: list of dict = {
##      count: [value, ...]
## }
## output: dict = {
##      count: [value, ...]
## }
def merge(dicts):
    unique_keys = set(key for dict in dicts for key in dict.keys())
    result = {key: [] for key in unique_keys}
    for key in unique_keys:
        for dict in dicts:
            result[key] = result[key] + dict.get(key, [])
    return result

def print_result(f):
    def inner_func(*args, **kwargs):
        result = f(*args, **kwargs)
        print("(ga profit, min:medium:max for ga, min:medium:max for heft, sigma ga / sigma heft, sigma ga/medium ga, sigma heft/medium heft, heft_results, ga_results)")
        print("==================================")
        print("=== " + str(args[0]))
        print("==================================")
        for (key, value) in result.items():
            print("{0}: {1}".format(key, value))
            # print("{0}: {1}, {2}, {3}".format(key, value[0], value[1], value[2]))
        return result
    return inner_func

def averaging_counts(count_dict):
    def get_data(items):
        avr = sum(items)/len(items)
        sigma = math.sqrt(sum([(it-avr)*(it-avr) for it in items])/len(items))
        return (avr, sigma)
    result = {count: get_data(items) for (count, items) in count_dict.items()}
    return result

##================================================
##=== Lets aggregate
##================================================



@print_result
def aggregate(wf_name):
    base_dir = "D:/testdir/_GA_VS_HEFT/exp1_1000"
    fs = [os.path.join(root, file) for root, dirs, files in os.walk(base_dir) for file in files if file.endswith(".txt") and wf_name in file]

    aggrs = [aggregate_fails(f) for f in fs]
    heft_clustering = merge([x[2] for x in aggrs])
    ga_clustering = merge([x[3] for x in aggrs])

    average_heft_clustering = averaging_counts(heft_clustering)
    average_ga_clustering = averaging_counts(ga_clustering)

    common_keys = set(key for key in average_heft_clustering.keys()) & set(key for key in average_ga_clustering.keys())

    result = dict()
    for key in common_keys:

        max_heft = max(heft_clustering[key])
        max_ga = max(ga_clustering[key])
        min_heft = min(heft_clustering[key])
        min_ga = min(ga_clustering[key])

        gm = average_ga_clustering[key][0]
        hm = average_heft_clustering[key][0]
        g = average_ga_clustering[key][1]
        h = average_heft_clustering[key][1]

        ## estimate what percent of median sigma is
        gp = (g/gm)*100
        hp = (h/hm)*100

        # additionally add results for counts with small number measurements
        hc = None
        gc = None

        if len(heft_clustering[key]) < 10:
            hc = heft_clustering[key]
        if len(ga_clustering[key]) < 10:
            gc = ga_clustering[key]

        #(ga profit, min:medium:max for ga, min:medium:max for heft, sigma ga / sigma heft, sigma ga/medium ga, sigma heft/medium heft, heft_results, ga_results)
        result[key] = ((1 - gm/hm)*100, str(min_ga)+":"+str(gm)+":"+str(max_ga), str(min_heft)+":"+str(hm)+":"+str(max_heft), str(g) + "/" + str(h), "ga: " + str(gp), "heft: " + str(hp), hc, gc)
    return result

wf_names = ["Montage_25", "Montage_30", "Montage_40", "Montage_50"]

[aggregate(wf_name) for wf_name in wf_names]












