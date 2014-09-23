from functools import partial
import os
import numpy
from heft.experiments.aggregate_utilities import WFS_COLORS, aggregate
import matplotlib.pyplot as plt
from heft.experiments.comparison_experiments.gaheft_series.visualization_utilities import confidence_aggr
from heft.settings import TEMP_PATH

POP_SIZE_COLORS = {
    #20: "-gD",
    #35: "-rD",
    50: "-yD",

    #60: "-gD",
    #105: "-rD",
    150: "-yD",
}

def extract_and_add(alg_name, data, d):
    """
    Intermediate data representation for analysis
    {
        "{wf_name}": {
            {pop_size}:[values..]
        }
    }
    """

    if d["params"]["alg_name"] != alg_name:
        return data

    wf_name = d["wf_name"]
    pop_size = d["params"]["alg_params"]["n"]
    makespan = d["result"]["makespan"]


    wf_results = data.get(wf_name, {})

    arr_data = wf_results.get(pop_size, [])
    arr_data.append(makespan)
    wf_results[pop_size] = arr_data

    data[wf_name] = wf_results
    return data


def plot_aggregate_results(data, alg_name, pop_size_colors=POP_SIZE_COLORS):

    # aggr = lambda results: numpy.mean(results)

    def get_points_format(data):
        if len(data) == 0:
            raise ValueError("data is empty")


        pop_size_count = len(data[next(iter(data))])

        if any((len(pop_sizes) != pop_size_count for wf_name, pop_sizes in data.items())):
            raise Exception("Inconsistency in pop_sizes.")

        points = []
        for wf_name in data:
            points.append((wf_name, -1.0))
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
    #ax.set_title(alg_name)
    ax.set_ylabel("makespan", fontsize=45)

    plt.tick_params(axis='both', which='major', labelsize=32)
    plt.tick_params(axis='both', which='minor', labelsize=32)

    pop_sizes = data[next(iter(data))]

    for pop_size in pop_sizes:
        if pop_size not in pop_size_colors:
            continue
        style = pop_size_colors[pop_size]
        points = []
        for wf_name, sizes in sorted(data.items(), key=lambda x: x[0]):
            points.append(confidence_aggr(sizes[pop_size]))

        plt.plot([i for i in range(0, len(points))], [x for x in points], style, label=pop_size, linewidth=4.0, markersize=10)


    plt.setp(plt.xticks()[1], rotation=30, ha='right')
    ax.legend()
    pass





if __name__ == "__main__":

    alg_names = ["pso"]

    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "migaheft_series", "migaheft_for_ga_common")
    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "migaheft_series", "migaheft_for_pso_[20, 35, 50]x[m25-m75]x50")
    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "migaheft_series", "migaheft_for_gsa_[20,35,50]x[m25-m75]x50")

    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "gaheft_60_series", "gaheft_for_ga_[60, 105, 150]x[m25-m75]x50")
    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "gaheft_60_series", "gaheft_for_gsa_[60, 105, 150]x[m25-m75]x50")
    path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "gaheft_60_series", "gaheft_for_pso_[60, 105, 150]x[m25-m75]x50")
    #
    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "gaheft_20_series", "gaheft_for_ga_[20, 35, 50]x[m25-m75]x50")
    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "gaheft_20_series", "gaheft_for_gsa_[20, 35, 50]x[m25-m75]x150")
    # path = os.path.join(TEMP_PATH, "compilation", "migaheft_gaheft_experiemnt", "gaheft_20_series", "gaheft_for_pso_[20, 35, 50]x[m25-m75]x150")

    for alg_name in alg_names:
        wf_plot = partial(plot_aggregate_results, alg_name=alg_name)
        extract = partial(extract_and_add, alg_name)
        picture_path = os.path.join("compilation", "pop_wf_{0}.png".format(alg_name))
        aggregate(pathes=[path],
                  picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])

