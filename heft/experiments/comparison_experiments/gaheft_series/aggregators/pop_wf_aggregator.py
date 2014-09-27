from functools import partial
import json
import os

import matplotlib.pyplot as plt

from heft.experiments.aggregate_utilities import aggregate, InMemoryDataAggregator
from heft.experiments.comparison_experiments.gaheft_series.aggregators.visualization_utilities import confidence_aggr
from heft.settings import TEMP_PATH
from heft.utilities.union_to_one_dir import generate_pathes


POP_SIZE_COLORS = {
     20: "-gD",
    # 35: "-rD",
    # 50: "-yD",

    # 60: "-gD",
    #105: "-rD",
    #150: "-yD",
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

    if d["params"]["estimator_settings"]["reliability"] != 0.95:
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


def plot_aggregate_profit_results(data, base_pop_size, pop_size_colors=POP_SIZE_COLORS):

    profit_aggr = lambda old, new: (old/new - 1)*100


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
    ax.set_ylabel("ptofit, %", fontsize=45)

    plt.tick_params(axis='both', which='major', labelsize=32)
    plt.tick_params(axis='both', which='minor', labelsize=32)

    pop_sizes = data[next(iter(data))]


    for pop_size in pop_sizes:
        if pop_size not in pop_size_colors:
            continue
        if pop_size == base_pop_size:
            continue
        style = pop_size_colors[pop_size]
        points = []
        for wf_name, sizes in sorted(data.items(), key=lambda x: x[0]):
            own = confidence_aggr(sizes[pop_size])
            base = confidence_aggr(data[wf_name][base_pop_size])
            profit = profit_aggr(base, own)
            points.append(profit)

        plt.plot([i for i in range(0, len(points))], [x for x in points], style, label=pop_size, linewidth=4.0, markersize=10)


    plt.setp(plt.xticks()[1], rotation=30, ha='right')
    ax.legend()
    pass

def plot_aggregate_results(data, pop_size_colors=POP_SIZE_COLORS):

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

        #alg_names = ["ga", "pso", "gsa"]
    # alg_names = ["pso"]

   # algs = {
        # "ga": [os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/migaheft_series/migaheft_for_ga_common"),
        #
        #        os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_ga_[m25,m40]_[0.95]"),
        #        os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_ga_m50_1000"),
        #        os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_ga_m75_1000"),
        #
        #        # os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/gaheft_60_series/gaheft_for_ga_[60, 105, 150]x[m25-m75]x50")
        #
        #        ],
        # "pso": [os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/migaheft_series/migaheft_for_pso_[20, 35, 50]x[m25-m75]x50"),
        #
        #        os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_pso_[m25,m40]_[0.95]"),
        #        os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_pso_m50_1000"),
        #        os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_pso_m75_1000"),
        #
        #        # os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/gaheft_60_series/gaheft_for_pso_[60, 105, 150]x[m25-m75]x50")
        #         ],
        # "gsa": [
        #     os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/migaheft_series/migaheft_for_gsa_[20,35,50]x[m25-m75]x50"),
        #     # os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/migaheft_20/migaheft_for_gsa"),
        #
        #         os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_gsa_[m25,m40]_[0.95]"),
        #         os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_gsa_m50_600"),
        #         os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_gsa_m75_1000"),
        #
        #         # os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/gaheft_60_series/gaheft_for_gsa_[60, 105, 150]x[m25-m75]x50")
        #         ]
   # }


    algs = {
        "pso": [os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/migaheft_series/migaheft_for_pso_[20, 35, 50]x[m25-m75]x50"),

                os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_pso]_[pso]_[Montage_25]_[n-20]_[rel-0.95]"),
            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_pso]_[pso]_[Montage_40]_[n-20]_[rel-0.95]"),
            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_pso]_[pso]_[Montage_50]_[n-20]_[rel-0.95]"),
            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_pso]_[pso]_[Montage_75]_[n-20]_[rel-0.95]"),
                ],
        "gsa": [
            os.path.join(TEMP_PATH, "compilation/migaheft_gaheft_experiemnt/migaheft_series/migaheft_for_gsa_[20,35,50]x[m25-m75]x50"),

            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_gsa]_[gsa]_[Montage_25]_[n-20]_[rel-0.95]"),
            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_gsa]_[gsa]_[Montage_40]_[n-20]_[rel-0.95]"),
            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_gsa]_[gsa]_[Montage_50]_[n-20]_[rel-0.95]"),
            os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted/[gaheft_for_gsa]_[gsa]_[Montage_75]_[n-20]_[rel-0.95]"),

                ]

    }

    # for alg_name, alg_path in algs.items():
    #     for file in os.listdir(alg_path[0]):
    #         pf = os.path.join(alg_path[0], file)
    #         with open(pf, "r") as f:
    #             data = json.load(f)
    #         data["params"]["alg_name"] = alg_name
    #         with open(pf, "w") as f:
    #             json.dump(data, f)


    # path = [os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_pso_m75_1000"),\
    #           os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_pso_m50_1000")]
    #path = [os.path.join(TEMP_PATH, "compilation/gaheft_for_pso_20")]

    #path = generate_pathes(os.path.join(TEMP_PATH, "compilation/gaheft_results_sorted"))

    # path = [os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_ga_m50_1000"),
    #           os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_ga_m75_1000"),
    #           os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_gsa_m50_600"),
    #           os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_gsa_m75_1000"),
    #           os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_pso_m50_1000"),
    #           os.path.join(TEMP_PATH, "compilation/ga_pso_gsa/gaheft_for_pso_m75_1000"),
    #
    #           os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_ga_[m25,m40]_[0.95]"),
    #           os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_gsa_[m25,m40]_[0.95]"),
    #           os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_pso_[m25,m40]_[0.95]"),
    #           ]





    for alg_name, pathes in algs.items():
        data_aggr = InMemoryDataAggregator(pathes)
        # wf_plot = partial(plot_aggregate_results)
        wf_plot = partial(plot_aggregate_profit_results, base_pop_size=20)
        extract = partial(extract_and_add, alg_name)
        picture_path = os.path.join("compilation", "pop_wf_{0}.png".format(alg_name))
        data_aggr(
                  picture_path=picture_path, extract_and_add=extract, functions=[wf_plot])

