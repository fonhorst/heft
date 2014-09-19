from functools import partial
import os
import numpy
from heft.experiments.aggregate_utilities import WFS_COLORS, aggregate
import matplotlib.pyplot as plt
from heft.settings import TEMP_PATH

POP_SIZE_COLORS = {
    20: "-gD",
    35: "-rD",
    50: "-yD"
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

    aggr = lambda results: numpy.mean(results)

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
    ax.set_title(alg_name)
    ax.set_ylabel("makespan")

    pop_sizes = data[next(iter(data))]

    for pop_size in pop_sizes:
        if pop_size not in pop_size_colors:
            continue
        style = pop_size_colors[pop_size]
        points = []
        for wf_name, sizes in sorted(data.items(), key=lambda x: x[0]):
            points.append(aggr(sizes[pop_size]))

        plt.plot([i for i in range(0, len(points))], [x for x in points], style, label=pop_size)


    plt.setp(plt.xticks()[1], rotation=30, ha='right')
    ax.legend()
    pass





if __name__ == "__main__":

    alg_names = ["pso"]

    path = os.path.join(TEMP_PATH, "migaheft_for_gsa_[20,35,50]x[m25-m50]x50")

    for alg_name in alg_names:
        wf_plot = partial(plot_aggregate_results, alg_name=alg_name)
        extract = partial(extract_and_add, alg_name)
        aggregate(path=path,
                  picture_path="migaheft_series_{0}.png".format(alg_name), extract_and_add=extract, functions=[wf_plot])

