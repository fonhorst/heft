from functools import partial
import functools
import operator
import os
from pprint import pprint

import numpy
import matplotlib.pyplot as plt

from heft.experiments.aggregate_utilities import InMemoryDataAggregator
from heft.settings import TEMP_PATH

UNKNOWN = "unknown"


ALG_COLORS = {
    "ga": "-gD",
    "heft": "-rD",
    "pso": "-yD",
    "gsa": "-mD",
    "cga": "-mD",
    UNKNOWN: "-bD"
}

aggr = lambda results: numpy.mean(results)


def extract_and_add(alg_name, data, d):
    """
    Intermediate data representation for analysis
    {

        "{wf_name}": {
            "reliability": {
                    "{value}": []
                },
        }
    }
    """

    if d["params"]["alg_name"] != alg_name:
        return data

    wf_name = d["wf_name"]
    if "estimator_settings" not in d["params"]:
        reliability = d["estimator_settings"]["reliability"]
    else:
        reliability = d["params"]["estimator_settings"]["reliability"]
    makespan = d["result"]["makespan"]
    failed_count = d["result"]["overall_failed_tasks_count"]


    rel_results = data.get(wf_name, {"reliability":{}})

    arr_data = rel_results["reliability"].get(reliability, [])
    arr_data.append((makespan, failed_count))
    rel_results["reliability"][reliability] = arr_data

    #pprint(rel_results["reliability"].keys())

    data[wf_name] = rel_results
    return data


def composite_extract_and_add(data, d, alg_names):
    alg_name = "cga" #d["params"]["alg_name"]

    if alg_name not in alg_names:
        return data

    alg_data = data.get(alg_name, {})
    extract_and_add(alg_name, alg_data, d)
    data[alg_name] = alg_data
    return data


def advanced_composite_extract_and_add(data, d, alg_names):
    if d["params"] is None:
        d["params"] = {"alg_name": UNKNOWN}

    alg_name = d["params"]["alg_name"]
    if alg_name != UNKNOWN and alg_name not in alg_names:
        return data

    alg_data = data.get(alg_name, {})
    extract_and_add(alg_name, alg_data, d)
    data[alg_name] = alg_data
    return data


def plot_aggregate_results(data, wf_name, alg_colors=ALG_COLORS, reliability=None):

    # aggr = lambda results: interval_statistics(results)[0]


    def get_points_format(data):
        if len(data) == 0:
            raise ValueError("data is empty")

        item = None
        for alg_name, itm in data.items():
            if wf_name in itm:
                item = itm
                break

        if item is None:
            raise ValueError("data don't contain iformation to plot according to wfs_colors limitations")

        points = []
        for value, results in item[wf_name]["reliability"].items():
            points.append((value, aggr(results)))
        points = sorted(points, key=lambda x: x[0])
        return points

    format_points = get_points_format(data) if reliability is None else [(str(r), 0) for r in reliability]

    plt.grid(True)
    ax = plt.gca()
    # + 1 for legend box
    ax.set_xlim(0, len(format_points) + 1)
    ax.set_xscale('linear')
    plt.xticks(range(0, len(format_points)))
    ax.set_xticklabels([p[0] for p in format_points])



    ax.set_title(wf_name, size=45)
    ax.set_ylabel("makespan", size=45)

    for alg_name, item in data.items():
        # wf_name = wf_name.split("_")[0]
        if alg_name not in alg_colors:
            continue
        style = alg_colors[alg_name]

        points = []
        for value, results in item[wf_name]["reliability"].items():
            if value in reliability or reliability is None:
                    points.append((value, aggr(results)))

        points = sorted(points, key=lambda x: x[0])



        plt.setp(plt.xticks()[1], rotation=30, ha='right')


        plt.plot([i for i in range(0, len(points))], [x[1] for x in points], style, label=alg_name, linewidth=4.0, markersize=10)

        plt.tick_params(axis='both', which='major', labelsize=32)
        plt.tick_params(axis='both', which='minor', labelsize=32)

        ax.legend()
    pass


def plot_aggregate_profit_results(data, wf_name, alg_colors=ALG_COLORS, reliability=None, base_alg_name=None):
    """
    plot comparison results relatively to base_alg_name.
    It will plot values only for a reliability which is present in results for each algorithm and wf
    :param data:
    :param wf_name:
    :param alg_colors:
    :param reliability:
    :param base_alg_name:
    :return:
    """

    ## data checking
    if len(data) == 0:
        raise ValueError("data is empty")

    for alg_name, itm in data.items():
        if wf_name not in itm:
            print("Warning! {0} is not found for {1}. Skip drawing it".format(wf_name, alg_name))
            #raise ValueError("data don't contain iformation to plot according to wfs_colors limitations")

    if base_alg_name is None:
        raise ValueError("base_alg_name cannot be None")

    ## let left only data which are in results for all algorithms
    reliability_values = (item[wf_name]['reliability'].keys() for alg_name, item in data.items() if alg_name in alg_colors)
    reliability_values = list(reliability_values)
    comparable_rel_values = functools.reduce(lambda x, y: set(x).intersection(y), reliability_values)
    comparable_rel_values = [rel for rel in comparable_rel_values if rel in reliability or reliability is None]

    ## aggregate data
    d = {alg_name: {} for alg_name in data}
    for alg_name, item in data.items():
        if alg_name not in alg_colors:
            continue

        for rel in comparable_rel_values:
            results =item[wf_name]["reliability"][rel]
            d[alg_name][rel] = aggr(results)

    format_points = [str(p) for p in sorted(comparable_rel_values)]

    #plt.grid(True)
    ax = plt.gca()
    # + 1 for legend box
    ax.set_xlim(0, len(format_points) + 1)
    ax.set_xscale('linear')
    plt.xticks(range(0, len(format_points)))
    ax.set_xticklabels([p[0] for p in format_points])

    if wf_name == "Montage_40":
        ax.set_title("Montage_35", size=45)
    else:
        ax.set_title(wf_name, size=45)

    ax.set_ylabel("profit, %", size=45)
    ax.set_xlabel("reliability", size=45)

    plt.tick_params(axis='both', which='major', labelsize=32)
    plt.tick_params(axis='both', which='minor', labelsize=32)

    for alg_name, item in d.items():
        if alg_name not in alg_colors:
            continue
        points = []
        style = alg_colors[alg_name]
        if alg_name == base_alg_name:
            continue
        for value, res in item.items():
            points.append((value, (1 - res/d[base_alg_name][value])*100))

        points = sorted(points, key=lambda x: x[0])
        plt.plot([i for i in range(0, len(points))], [x[1] for x in points], style,
                 label=alg_name, linewidth=4.0, markersize=10)
        ax.legend()
    pass


if __name__ == "__main__":

    algs = {
        "cga": [r"D:\wspace\gaheft_series_Misha\cga_dynamic"],
        "heft": [os.path.join(TEMP_PATH, "gaheft_for_heft_new_500")],
    }

    wf_names = ["Montage_50", "Montage_75"]
    reliability = [0.9, 0.925, 0.95, 0.975, 0.99]

    pathes = functools.reduce(operator.add, algs.values(), [])
    data_aggr = InMemoryDataAggregator(pathes)
    wf_names = sorted(wf_names)

    plt.figure(figsize=(10, 10))
    for wf_name in wf_names:

        wf_plot = partial(plot_aggregate_profit_results, wf_name=wf_name, reliability=reliability, base_alg_name="heft")
        extract = partial(advanced_composite_extract_and_add, alg_names=algs.keys())

        names = functools.reduce(operator.add, ("_" + alg_name for alg_name in algs.keys()), "")
        picture_path = os.path.join("D:/wspace/gaheft_series_Misha", "gaheft_series_for{0}_{1}.png".format(names, wf_name))
        data_aggr(picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])

