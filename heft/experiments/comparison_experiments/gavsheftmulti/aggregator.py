from functools import partial
import json
import os
import matplotlib.pyplot as plt
import numpy

path = ""



wfs_colors = {
    "Montage": "-gD",
    "CyberShake": "-rD",
    "Inspiral": "-bD",
    "Sipht": "-yD",
    "Epigenomics": "-fD"
}


def extract_and_add(data, d):
    """
    Intermediate data representation for analysis
    {
        "{wf_name}": {
            "ideal_flops": {
                "{value}": []
            },
            "transfer_time": {
                "{value}": []
            }
        }
    }
    """
    wf_name = d["wf_name"]
    ideal_flops = d["params"]["ideal_flops"]
    transfer_time = d["params"]["transfer_time"]
    heft_makespan = d["heft"]["makespan"]
    ga_makespan = d["ga"]["makespan"]


    default_entry = {"ideal_flops": {}, "transfer_time": {}}
    el = data.get(wf_name, default_entry)


    iflops_results = el["ideal_flops"].get(ideal_flops, [])
    iflops_results.append({"h": heft_makespan, "g": ga_makespan})
    el["ideal_flops"][ideal_flops] = iflops_results

    transfer_results = el["transfer_time"].get(transfer_time, [])
    transfer_results.append({"h": heft_makespan, "g": ga_makespan})
    el["transfer_time"][transfer_time] = transfer_results

    data[wf_name] = el

    return data

def plot_aggregate_results(data, property_name):

    aggr = lambda results: (1 - numpy.mean([r["g"] for r in results])/numpy.mean([r["h"] for r in results]))*100
    for wf_name, item in data.items():
        points = []
        for value, results in item[property_name].items():
            points.append((value, aggr(results)))

        points = sorted(points, key=lambda x: x[0])

        plt.grid(True)
        ax = plt.gca()
        ax.set_xlim(0, len(points))
        ax.set_xscale('linear')
        plt.xticks(range(0, len(points)))
        ax.set_xticklabels(points)
        ax.set_title(property_name)
        ax.set_ylabel("profit, %")
        # plt.setp(plt.xticks()[1], rotation=30, ha='right')


        # gens = sorted(data["iterations"], key=lambda x: x["gen"])
        # bests = [(points.index(gen["gen"]), -1*gen["solsstat"][0]["best"]) for gen in gens if gen["gen"] in points]

        wf_name = wf_name.split("_")[0]
        plt.plot([x[0] for x in points], [x[1] for x in points], wfs_colors[wf_name])
    pass

transfer_plot = partial(plot_aggregate_results, property_name="transfer_time")
iflops_plot = partial(plot_aggregate_results, property_name="ideal_flops")


def visualize(data, functions, path_to_save=None):
    plt.figure(figsize=(10, 10))

    for i in range(len(functions)):
        plt.subplot(len(functions), 1, i + 1)
        functions[i](data)

    plt.tight_layout()

    if path_to_save is None:
        plt.show()
    else:
        plt.savefig(path_to_save, dpi=96.0, format="png")
        plt.clf()
    pass

if __name__ == "__main__":
    files = [os.path.join(path, p) for p in os.listdir(path) if p.endswith(".json")]
    data = {}
    for p in files:
        with open(p, "r") as f:
            d = f.read()
            d = json.load(d)
            extract_and_add(data, d)

    visualize(data, [transfer_plot, iflops_plot])







