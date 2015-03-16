from functools import partial
import functools
import operator
import os
import numpy
import pylab
from heft.experiments.aggregate_utilities import WFS_COLORS, aggregate, interval_statistics, InMemoryDataAggregator
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
    wf_name = "Montage_75"
    reliabilities = [0.99, 0.975, 0.95, 0.925, 0.9]
    base_picture_path = os.path.join(TEMP_PATH, "compilation", "pso_gsa", "histograms")

    algs = {
        "heft": os.path.join(TEMP_PATH, "compilation", "gaheft_for_heft_new_500"),
       # "pso": os.path.join(TEMP_PATH, "compilation", "gaheft_for_gsa_m50_m75", "all"),
        "gsa": os.path.join(TEMP_PATH, "compilation", "pso_gsa", "gaheft_for_gsa_m75_1000")
    }

    pathes = functools.reduce(operator.add, algs.values(), [])
    data_aggr = InMemoryDataAggregator(pathes)

    for alg_name, alg_path in algs.items():

        for reliability in reliabilities:

            for wf_name in wf_names:
            extract = partial(extract_and_add, alg_name, wf_name, reliability)
            wf_plot = partial(plot_aggregate_results, wf_name)


                picture_path = os.path.join(base_picture_path, "hist_gaheft_for_{0}_{1}_{2}.png".format(alg_name, wf_name, reliability))

                data_aggr(picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])
