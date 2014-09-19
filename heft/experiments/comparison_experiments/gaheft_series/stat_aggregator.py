from functools import partial
import os
import numpy
import pylab
from heft.experiments.aggregate_utilities import WFS_COLORS, aggregate, interval_statistics
import matplotlib.pyplot as plt
from heft.settings import TEMP_PATH

ALG_COLORS = {
    # "ga": "-gD",
    "heft": "-rD",
    # "pso": "-yD",
    "gsa": "-mD"
}

REALIBILITIES = [0.99, 0.975, 0.95, 0.925, 0.9]

def extract_and_add(alg_name, wf_name, reliability, data, d):
    if d["wf_name"] != wf_name:
        return data
    if d["params"]["alg_name"] != alg_name:
        return data
    if d["params"]["estimator_settings"]["reliability"] != reliability:
        return data
    wf_data = data.get(wf_name, [])
    wf_data.append(d)
    data[wf_name] = wf_data
    return data


def plot_aggregate_results(wf_name, data):

    aggr = lambda results: int(interval_statistics(results if len(results) > 0 else [0.0])[0])
    # aggr = lambda results: len(results)

    data = data[wf_name]

    bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    value_map = {b: [] for b in bins}

    for d in data:
        fcount = d["result"]["overall_failed_tasks_count"]
        makespan = d["result"]["makespan"]
        value_map[fcount].append(makespan)

    values = [bin for bin, values in sorted(value_map.items(), key=lambda x: x[0]) for _ in values]



    plt.grid(True)

    n, bins, patches = pylab.hist(values, bins, histtype='stepfilled')
    pylab.setp(patches, 'facecolor', 'g', 'alpha', 0.75)


    values = [aggr(values) for bin, values in sorted(value_map.items(), key=lambda x: x[0])]
    rows = [[str(v) for v in values]]

    the_table = plt.table(cellText=rows,
                      rowLabels=None,
                      colLabels=bins,
                      loc='bottom')

    pass








if __name__ == "__main__":
    wf_name = "Montage_50"
    alg_name = "gsa"
    reliability = 0.99

    #path = os.path.join(TEMP_PATH, "all_results_sorted_and_merged", "gaheft_0.99-0.9_series")
    gsa_path = os.path.join(TEMP_PATH, "all_gsa_series_")
    heft_path = os.path.join(TEMP_PATH, "gaheft_for_heft_1000")

    path = gsa_path

    extract = partial(extract_and_add, alg_name, wf_name, reliability)
    wf_plot = partial(plot_aggregate_results, wf_name)


    picture_path = os.path.join(TEMP_PATH, "hist_gaheft_for_{0}_{1}.png".format(alg_name, wf_name))

    aggregate(pathes=[path],
                  picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])
