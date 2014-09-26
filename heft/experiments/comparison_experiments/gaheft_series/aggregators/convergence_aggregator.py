from functools import partial
import os

from matplotlib import pyplot as plt

from heft.experiments.aggregate_utilities import aggregate
from heft.experiments.comparison_experiments.gaheft_series.aggregators.visualization_utilities import confidence_aggr
from heft.settings import TEMP_PATH


def extract_and_add(wf_name, data, d):
    wf_data = data.get(wf_name, [])
    wf_data.append(d)
    data[wf_name] = wf_data
    return data


def plot_aggregate_igaheft_results(data, wf_name, task_id, points, draw_percents=False):

    data = data[wf_name]

    def update_dict(dt, elements):
        for gen, res in elements:
            el = dt.get(gen, [])
            el.append(res)
            dt[gen] = el
        return dt

    def extract_for_points(logbook_type):
        #random_points = {}
        inherited_points = {}
        for d in data:
            if d["params"]["executor_params"]["task_id_to_fail"] != task_id:
                continue
            inh_mins = [(el["gen"], el["min"]) for el in d["result"][logbook_type] if el["gen"] in points]
            update_dict(inherited_points, inh_mins)
            # rand_mins = [(el["gen"], el["min"]) for el in d["result"]["random_init_logbook"] if el["gen"] in points]
            # update_dict(random_points, rand_mins)
        # aggr = lambda results: numpy.mean(results)
        return [res for gen, res in sorted(((gen, confidence_aggr(results)) for gen, results in inherited_points.items()), key=lambda x: x[0])]

    plt.grid(True)
    ax = plt.gca()
    # + 1 for legend box
    ax.set_xlim(0, len(points) + 1)
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title(wf_name, size=45)
    ax.set_ylabel("makespan", size=45)

    ax.set_xlabel("iteration")

    inherited_based_values = extract_for_points("inherited_init_logbook")
    random_based_values = extract_for_points("random_init_logbook")

    plt.tick_params(axis='both', which='major', labelsize=32)
    plt.tick_params(axis='both', which='minor', labelsize=32)

    plt.setp(plt.xticks()[1], rotation=30, ha='right')

    if not draw_percents:
        ax.set_ylabel("makespan")
        plt.plot([i for i in range(0, len(points))], [x for x in inherited_based_values], "-gD", label="inherited", linewidth=4.0, markersize=10)
        plt.plot([i for i in range(0, len(points))], [x for x in random_based_values], "-rD", label="random", linewidth=4.0, markersize=10)

    else:
        ax.set_ylabel("profit, %")
        profit = lambda inh, rand: (rand / inh - 1) * 100
        plt.plot([i for i in range(0, len(points))], [profit(inh, rand) for inh, rand in zip(inherited_based_values, random_based_values)], "-bD", label="profit by inherited", linewidth=4.0, markersize=10)

    ax.legend()
    pass

if __name__ == "__main__":

    algs = {
        "ga": os.path.join(TEMP_PATH, "old", "all_results_sorted_and_merged", "igaheft_series", "igaheft_for_ga_m75x100"),
        "pso": os.path.join(TEMP_PATH, "old", "all_results_sorted_and_merged", "igaheft_series", "igaheft_for_pso_m75x50"),
        "gsa": os.path.join(TEMP_PATH, "old", "all_results_sorted_and_merged", "igaheft_series", "igaheft_for_gsa_m75x100")
    }

    wf_name = "Montage_75"
    points = [0, 1, 5, 10, 15, 20, 25, 50, 75, 100, 150, 200, 250, 299]
    task_ids = ["ID00000_000", "ID00010_000", "ID00020_000", "ID00040_000",
                    "ID00050_000", "ID00070_000"]

    extract = partial(extract_and_add, wf_name)

    for alg_name, path in algs.items():
        for task_id in task_ids:
            wf_plot = partial(plot_aggregate_igaheft_results, wf_name=wf_name, task_id=task_id, points=points, draw_percents=False)
            picture_path = os.path.join(TEMP_PATH, "compilation", "igaheft_series",  "igaheft_series_{0}_{1}.png".format(alg_name, task_id))
            aggregate(pathes=[path], picture_path=picture_path,
                      extract_and_add=extract, functions=[wf_plot])