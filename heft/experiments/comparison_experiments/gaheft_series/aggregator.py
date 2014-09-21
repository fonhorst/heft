from functools import partial
import os
import numpy
from heft.experiments.aggregate_utilities import WFS_COLORS, aggregate, interval_statistics
import matplotlib.pyplot as plt
from heft.experiments.comparison_experiments.gaheft_series.visualization_utilities import confidence_aggr
from heft.settings import TEMP_PATH

ALG_COLORS = {
    # "ga": "-gD",
    "heft": "-rD",
    "pso": "-yD",
    # "gsa": "-mD"
}



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
    reliability = d["params"]["estimator_settings"]["reliability"]
    makespan = d["result"]["makespan"]


    rel_results = data.get(wf_name, {"reliability":{}})

    arr_data = rel_results["reliability"].get(reliability, [])
    arr_data.append(makespan)
    rel_results["reliability"][reliability] = arr_data

    data[wf_name] = rel_results
    return data


def composite_extract_and_add(data, d, alg_names):
    alg_name = d["params"]["alg_name"]
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
            points.append((value, confidence_aggr(results)))
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
    ax.set_title(wf_name)
    ax.set_ylabel("makespan")

    for alg_name, item in data.items():
        # wf_name = wf_name.split("_")[0]
        if alg_name not in alg_colors:
            continue
        style = alg_colors[alg_name]

        points = []
        for value, results in item[wf_name]["reliability"].items():
            if value in reliability or reliability is None:
                points.append((value, confidence_aggr(results)))

        points = sorted(points, key=lambda x: x[0])



        plt.setp(plt.xticks()[1], rotation=30, ha='right')


        plt.plot([i for i in range(0, len(points))], [x[1] for x in points], style, label=alg_name)

        # plt.errorbar([i for i in range(0, len(points))], [x[1][0] for x in points],
        #              yerr=([x[1][4] for x in points],
        #                         [x[1][5] for x in points]))

        # for i, p in zip(range(0, len(points)), points):
        #     val, stat = p
        #     mean, mn, mx, std, left, right = stat
        #     plt.errorbar(i, mean, yerr=[left, right])


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
            points.append((value, confidence_aggr(results)))
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
    ax.set_title(wf_name, size=22)
    ax.set_ylabel("profit, %", size=20)
    ax.set_xlabel("reliability", size=20)

    plt.tick_params(axis='both', which='major', labelsize=18)
    plt.tick_params(axis='both', which='minor', labelsize=18)



    if base_alg_name is None:
        raise ValueError("base_alg_name cannot be None")

    d = {alg_name: { wf_name: {} } for alg_name in data}
    for alg_name, item in data.items():
        # wf_name = wf_name.split("_")[0]
        if alg_name not in alg_colors:
            continue

        for value, results in item[wf_name]["reliability"].items():
            if value in reliability or reliability is None:
                d[alg_name][wf_name][value] = confidence_aggr(results)
                # points.append((value, aggr(results)))

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
    # wf_names = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
    wf_names = ["Montage_75"]
    # wf_names = ["Montage_25"]
    # wf_names = ["Montage_25", "Montage_40", "Montage_50"]
    for wf_name in wf_names:
        # alg_names = ["ga", "heft"]
        alg_names = ["pso", "heft"]
        # alg_names = ["gsa", "heft"]

        path = os.path.join(TEMP_PATH, "all_results_sorted_and_merged", "gaheft_0.99-0.9_series")
        # alg_1_path = os.path.join(TEMP_PATH, "all_gsa_series_")
        # alg_1_path = os.path.join(TEMP_PATH, "all_gaheft_series_m25")

        # alg_1_path = os.path.join(TEMP_PATH, "new_gaheft", "gaheft_for_ga")
        # alg_1_path = os.path.join(TEMP_PATH, "new_gaheft", "gaheft_for_pso")
        # alg_1_path = os.path.join(TEMP_PATH, "new_gaheft", "gaheft_for_gsa")

        # alg_1_path = os.path.join(TEMP_PATH, "new_gaheft", "gaheft_for_pso_new")
        # alg_1_path = os.path.join(TEMP_PATH, "new_gaheft", "gaheft_for_gsa_new")

        # alg_1_path = os.path.join(TEMP_PATH, "new_gaheft_2", "gaheft_for_gsa_m75")
        alg_1_path = os.path.join(TEMP_PATH, "new_gaheft_2", "gaheft_for_pso_m75")

        # alg_1_path = os.path.join(path, "gaheft_for_ga_[0.99-0.9]x[m25-m75]x50")
        # alg_1_path = os.path.join(path, "gaheft_for_pso_[0.99-0.9]x[m25-m75]x50")
        # alg_1_path = os.path.join(path, "gaheft_for_gsa_[0.99-0.9]x[m25-m75]x100")

        # alg_2_path = os.path.join(path, "gaheft_for_heft_[0.99-0.9]x[m25-m75]x200")
        alg_2_path = os.path.join(TEMP_PATH, "gaheft_for_heft_1000")
        # wf_plot = partial(plot_aggregate_results, wf_name=wf_name, reliability=[0.9, 0.925, 0.95, 0.975, 0.99], )
        wf_plot = partial(plot_aggregate_profit_results, wf_name=wf_name, reliability=[0.9, 0.925, 0.95, 0.975, 0.99], base_alg_name="heft")
        extract = partial(composite_extract_and_add, alg_names=alg_names)
        # picture_path = os.path.join("all_results_sorted_and_merged", "gaheft_0.99-0.9_series", "gaheft_series_for_{0}_{1}.png".format(alg_names[0], wf_name))
        picture_path = os.path.join(TEMP_PATH, "new_gaheft_2", "gaheft_series_for_{0}_{1}.png".format(alg_names[0], wf_name))
        # picture_path = os.path.join(TEMP_PATH, "gaheft_series_for_{0}_{1}.png".format(alg_names[0], wf_name))
        aggregate(pathes=[alg_1_path, alg_2_path],
                  picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])
