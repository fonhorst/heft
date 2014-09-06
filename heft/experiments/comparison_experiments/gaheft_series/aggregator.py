from functools import partial
import os
import numpy
from heft.experiments.aggregate_utilities import WFS_COLORS, aggregate
import matplotlib.pyplot as plt
from heft.settings import TEMP_PATH

ALG_COLORS = {
    "ga": "-gD",
    "heft": "-rD",
    "pso": "-yD"
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


def plot_aggregate_results(data, wf_name, alg_colors=ALG_COLORS):

    aggr = lambda results: numpy.mean(results)

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

    format_points = get_points_format(data)

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
        style = alg_colors[alg_name]

        points = []
        for value, results in item[wf_name]["reliability"].items():
            points.append((value, aggr(results)))

        points = sorted(points, key=lambda x: x[0])



        plt.setp(plt.xticks()[1], rotation=30, ha='right')


        plt.plot([i for i in range(0, len(points))], [x[1] for x in points], style, label=alg_name)
        ax.legend()
    pass



if __name__ == "__main__":
    wf_name = "Montage_25"
    alg_names = ["ga", "heft"]
    path = os.path.join(TEMP_PATH, "to_analysis", "ga_and_heft_and_pso")
    wf_plot = partial(plot_aggregate_results, wf_name=wf_name)
    extract = partial(composite_extract_and_add, alg_names=alg_names)
    aggregate(path=path,
              picture_path="gaheft_series.png",extract_and_add=extract, functions=[wf_plot])