from functools import partial
import os
from matplotlib import pyplot as plt
import numpy
from heft.experiments.aggregate_utilities import aggregate
from heft.settings import TEMP_PATH


def extract_and_add(wf_name, data, d):
    wf_data = data.get(wf_name, [])
    wf_data.append(d)
    data[wf_name] = wf_data
    return data


def plot_aggregate_igaheft_results(data, wf_name, task_id, points):

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
        aggr = lambda results: numpy.mean(results)
        return [res for gen, res in sorted(((gen, aggr(results)) for gen, results in inherited_points.items()), key=lambda x: x[0])]

    plt.grid(True)
    ax = plt.gca()
    # + 1 for legend box
    ax.set_xlim(0, len(points) + 1)
    ax.set_xscale('linear')
    plt.xticks(range(0, len(points)))
    ax.set_xticklabels(points)
    ax.set_title(wf_name)
    ax.set_ylabel("makespan")
    ax.set_xlabel("iteration")

    inherited_based_values = extract_for_points("inherited_init_logbook")
    random_based_values = extract_for_points("random_init_logbook")

    plt.setp(plt.xticks()[1], rotation=30, ha='right')

    plt.plot([i for i in range(0, len(points))], [x for x in inherited_based_values], "-gD", label="inherited")
    plt.plot([i for i in range(0, len(points))], [x for x in random_based_values], "-rD", label="random")
    ax.legend()

    pass

if __name__ == "__main__":
    wf_name = "Montage_75"
    points = [0, 1, 5, 10, 15, 20, 25, 50, 75, 100, 150, 200, 250, 299]
    task_ids = ["ID00000_000", "ID00010_000", "ID00020_000", "ID00040_000",
                    "ID00050_000", "ID00070_000"]

    # path = os.path.join(TEMP_PATH, "igaheft_analysis", "igaheft_for_ga_m75_20")
    # path = os.path.join(TEMP_PATH, "igaheft_analysis", "igaheft_for_pso_m75_20")
    # path = os.path.join(TEMP_PATH, "igaheft_analysis", "igaheft_for_gsa_m75_5")
    path = os.path.join(TEMP_PATH, "all_results_sorted_and_merged", "igaheft_series", "igaheft_for_gsa_m75x100")


    extract = partial(extract_and_add, wf_name)

    for task_id in task_ids:
        wf_plot = partial(plot_aggregate_igaheft_results, wf_name=wf_name, task_id=task_id, points=points)
        picture_path = os.path.join(TEMP_PATH, "all_results_sorted_and_merged", "igaheft_series",  "igaheft_series_{0}.png".format(task_id))
        aggregate(pathes=[path], picture_path=picture_path,
                  extract_and_add=extract, functions=[wf_plot])