from functools import partial
import functools
import operator
import os
from pprint import pprint

import numpy
import matplotlib.pyplot as plt

from heft.experiments.aggregate_utilities import InMemoryDataAggregator
from heft.settings import TEMP_PATH


ALG_COLORS = {
    "ga": "-gD",
    "heft": "-rD",
    "cpso": "-yD",
    "cgsa": "-mD",
    "cga": "-mD"
}

# aggr = confidence_aggr
aggr = lambda results: numpy.mean(results)


# def aggr(results):
#     counts_arr = [[] for _ in range(15)]
#     for makespan, failed_count in results:
#         if len(counts_arr) < failed_count:
#             for _ in range(failed_count - len(counts_arr)):
#                 counts_arr.append([])
#         counts_arr[failed_count].append(makespan)
#
#     interval_statistics([for counts_arr])





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
        d["params"] = {"alg_name": "cga"}

    alg_name = d["params"]["alg_name"]
    if alg_name not in alg_names:
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

        # plt.errorbar([i for i in range(0, len(points))], [x[1][0] for x in points],
        #              yerr=([x[1][4] for x in points],
        #                         [x[1][5] for x in points]))

        # for i, p in zip(range(0, len(points)), points):
        #     val, stat = p
        #     mean, mn, mx, std, left, right = stat
        #     plt.errorbar(i, mean, yerr=[left, right])

        plt.tick_params(axis='both', which='major', labelsize=32)
        plt.tick_params(axis='both', which='minor', labelsize=32)

        ax.legend()
    pass


def plot_aggregate_profit_results(data, wf_name, alg_colors=ALG_COLORS, reliability=None, base_alg_name=None):

    # aggr = lambda results: numpy.mean(results)

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

    # pprint(data)

    plt.grid(True)
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



    if base_alg_name is None:
        raise ValueError("base_alg_name cannot be None")

    d = {alg_name: { wf_name: {} } for alg_name in data}


    # print("===========================")
    # print("===========================")
    # print("===========================")
    #
    #pprint(data)

    for alg_name, item in data.items():
        # wf_name = wf_name.split("_")[0]
        if alg_name not in alg_colors:
            continue

        for value, results in item[wf_name]["reliability"].items():
            if value in reliability or reliability is None:
                d[alg_name][wf_name][value] = aggr(results)
                # points.append((value, aggr(results)))

    print("===========================")
    print("===========================")
    print("===========================")
    pprint(d)


    kr = {alg_name: { wf_name: {} } for alg_name in data}
    for alg_name, item in data.items():
        # wf_name = wf_name.split("_")[0]
        if alg_name not in alg_colors:
            continue

        for value, results in item[wf_name]["reliability"].items():
            if value in reliability or reliability is None:
                kr[alg_name][wf_name][value] = len(results)


    print("===========================")
    print("===========================")
    print("===========================")
    pprint(kr)

    for alg_name, item in d.items():
        if alg_name not in alg_colors:
            continue
        points = []
        style = alg_colors[alg_name]
        if alg_name == base_alg_name:
            continue
        for value, res in item[wf_name].items():
            points.append((value, (1 - res/d[base_alg_name][wf_name][value])*100))


        points = sorted(points, key=lambda x: x[0])
        #plt.setp(plt.xticks()[1], rotation=30, ha='right')
        plt.plot([i for i in range(0, len(points))], [x[1] for x in points], style, label=alg_name, linewidth=4.0, markersize=10)
        ax.legend()
    pass


if __name__ == "__main__":


    # algs = {
    #     "ga": [os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_ga_[0.99-0.9]x[m25-m75]x50")],
    #     #"pso": os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_pso_[0.99-0.9]x[m25-m75]x50"),
    #     # "gsa": os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_gsa_[0.99-0.9]x[m25-m75]x100"),
    #     "heft": [os.path.join(TEMP_PATH, "compilation/gaheft_for_heft_new_500")],
    # }

    algs = {
        # "ga": [os.path.join(TEMP_PATH, "compilation/gaheft_[ga,pso,gsa]_[0.9-0.99]")],
        # "pso": [os.path.join(TEMP_PATH, "compilation/gaheft_[ga,pso,gsa]_[0.9-0.99]")],
        #"cga": [os.path.join(TEMP_PATH, "cga_dynamic_results")],
        "cga": [r"D:/Projects/heft/temp/result/cpso/cook"],
        #"gsa": [os.path.join(TEMP_PATH, "compilation/gaheft_[ga,pso,gsa]_[0.9-0.99]")],
        # "heft": [os.path.join(TEMP_PATH, "gaheft_for_heft_new_500")],
        "heft": [r"D:/Projects/heft/temp/gaheft_for_heft_new_500"]
    }
    # wf_names = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
    # wf_names = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
    wf_names = ["Montage_25"]
    # wf_names = ["Montage_40", "Montage_50", "Montage_75"]
    # wf_names = ["Montage_75"]


    pathes = functools.reduce(operator.add, algs.values(), [])
    data_aggr = InMemoryDataAggregator(pathes)
    for wf_name in wf_names:

        # wf_plot = partial(plot_aggregate_results, wf_name=wf_name, reliability=[0.9, 0.925, 0.95, 0.975, 0.99], )
        # reliability=[0.925]
        reliability=[0.9, 0.925, 0.95, 0.975, 0.99]
        # reliability=[0.975]
        wf_plot = partial(plot_aggregate_profit_results, wf_name=wf_name, reliability=reliability, base_alg_name="heft")
        extract = partial(advanced_composite_extract_and_add, alg_names=algs.keys())

        names = functools.reduce(operator.add, ("_" + alg_name for alg_name in algs.keys()), "")
        # picture_path = os.path.join(TEMP_PATH, "gaheft_series_for{0}_{1}.png".format(names, wf_name))
        picture_path = os.path.join("D:/Projects/heft/temp/result/cpso/", "gaheft_series_for{0}_{1}.png".format(names, wf_name))
        data_aggr(picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])
