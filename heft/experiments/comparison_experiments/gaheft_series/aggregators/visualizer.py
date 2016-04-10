from itertools import groupby
import json
import os
from statistics import mean
from matplotlib import pyplot


def json_files(directory):
    """
    recursively find all files which contain all necessary data for visualization of certain alg
    (in json format)
    :param directory: root path where to start
    :return: array of full pathes
    """
    files = [os.path.join(root, json_file) for root, subdirs, files in os.walk(directory)
             for json_file in files if json_file.endswith(".txt")]
    return files


def combine_results(paths):
    """
     merge all files, extract needed fields and classify by workflow:
    {
        "experiment_name": {
            "alg_name": {
                "wf_name": {
                    "reliability_value": {
                        "mean_value": "value",
                        "values_set": [("fail_count", "value")]
                    }
                }
            }
        }
    }
    :param paths: array of full pathes
    :return: dict with the according structure
    """

    def extract_data(path):
        with open(path, "r") as f:
            jsonlines = f.readlines()
        data = (json.loads(jsonline) for jsonline in jsonlines)
        return data

    def by_experiment_name(record): return record["params"]["experiment_name"]

    def by_alg_name(record): return record["params"]["alg_name"]

    def by_wf_name(record): return record["wf_name"]

    def by_reliability(record): return record["params"]["estimator_settings"]["reliability"]

    def extract_results(record_iter):
        values_array = [(record["result"]["makespan"],
                         record["result"]["overall_failed_tasks_count"])for record in record_iter]

        mean_makespan = mean((makespan for makespan, _ in values_array))

        return {
            "mean": mean_makespan,
            # "values": values_array,
        }

    experiments = (record for path in paths for record in extract_data(path))

    experiments = {
        exp_name: {
            alg_name: {
                wf_name: {
                    reliability: extract_results(g3)
                    for reliability, g3 in groupby(sorted(g2, key=by_reliability), by_reliability)
                }
                for wf_name, g2 in groupby(sorted(g1, key=by_wf_name), by_wf_name)
            }
            for alg_name, g1 in groupby(sorted(g, key=by_alg_name), by_alg_name)
        }
        for exp_name, g in groupby(sorted(experiments, key=by_experiment_name), by_experiment_name)
    }

    return experiments


def visualize(experiments, directory_to_save=None):
    """
    takes a dict which is in format
    described for combine_results
    and draws combined picture
    :param experiments:
    :return: nothing
    """

    wf_names = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]
    reliability = ["0.9", "0.925", "0.95", "0.975", "0.99"]

    if not os.path.exists(directory_to_save):
        os.makedirs(directory_to_save)

    def common_settings():
        pyplot.grid(True)
        ax = pyplot.gca()
        ax.set_xlim(0, len(reliability))
        ax.set_xscale('linear')
        pyplot.xticks(range(0, len(reliability)))
        ax.set_xticklabels(reliability)
        ax.set_ylabel("profit, %", fontsize=45)
        ax.set_xlabel("reliability", fontsize=45)
        pyplot.tick_params(axis='both', which='major', labelsize=32)
        pyplot.tick_params(axis='both', which='minor', labelsize=32)
        pass

    for i, wf_name in enumerate(wf_names):

        pyplot.figure(i, figsize=(18, 12))
        common_settings()

        heft_line = experiments["gaheft_for_heft"]["heft"][wf_name]
        ga_line = experiments["gaheft_for_ga"]["ga"][wf_name]
        pso_line = experiments["gaheft_for_pso"]["pso"][wf_name]

        heft_line = [heft_line[rel]["mean"] for rel in reliability]
        ga_line = [ga_line[rel]["mean"] for rel in reliability]
        pso_line = [pso_line[rel]["mean"] for rel in reliability]

        ga_line = [((heft_value/ga_value) - 1)*100 for ga_value, heft_value in zip(ga_line, heft_line)]
        pso_line = [((heft_value/pso_value) - 1)*100 for pso_value, heft_value in zip(pso_line, heft_line)]

        pyplot.plot(ga_line, "-gD", linewidth=4.0, markersize=10)
        pyplot.plot(pso_line, "-yD", linewidth=4.0, markersize=10)

        pyplot.savefig(os.path.join(directory_to_save, str(wf_name) + ".png"),
                       label="recalculation",
                       dpi=96.0,
                       format="png")

    pass


def visualize_migaheft(experiments, directory_to_save=None):
    """
    takes a dict which is in format
    described for combine_results
    and draws combined picture
    :param experiments:
    :return: nothing
    """

    wf_names = ["Montage_25", "Montage_40", "Montage_50", "Montage_75"]

    if not os.path.exists(directory_to_save):
        os.makedirs(directory_to_save)

    def common_settings():
        pyplot.grid(True)
        ax = pyplot.gca()
        ax.set_xlim(0, len(wf_names))
        ax.set_xscale('linear')
        pyplot.xticks(range(0, len(wf_names)))
        ax.set_xticklabels(wf_names)
        ax.set_ylabel("profit, %", fontsize=45)
        ax.set_xlabel("Workflow", fontsize=45)
        pyplot.tick_params(axis='both', which='major', labelsize=32)
        pyplot.tick_params(axis='both', which='minor', labelsize=32)
        pass

    miga = []
    mipso = []

    for i, wf_name in enumerate(wf_names):

        pyplot.figure(i, figsize=(18, 12))
        common_settings()

        # ga_line = experiments["gaheft_for_ga"]["ga"][wf_name]
        # pso_line = experiments["gaheft_for_pso"]["pso"][wf_name]

        heft_line = experiments["gaheft_for_heft"]["heft"][wf_name]

        ga_line = heft_line
        pso_line = heft_line

        miga_line = experiments["migaheft_for_ga"]["ga"][wf_name]
        mipso_line = experiments["migaheft_for_pso"]["pso"][wf_name]

        miga_value = (ga_line["0.95"]["mean"] / miga_line["0.95"]["mean"] - 1) * 100
        mipso_value = (pso_line["0.95"]["mean"] / mipso_line["0.95"]["mean"] - 1) * 100

        miga.append(miga_value)
        mipso.append(mipso_value)

    pyplot.plot(miga, "-gD", linewidth=4.0, markersize=10)
    pyplot.plot(mipso, "-yD", linewidth=4.0, markersize=10)

    pyplot.savefig(os.path.join(directory_to_save, "migaheft.png"),
                   label="recalculation",
                   dpi=96.0,
                   format="png")
    pass


def extract_and_save(input_path, output_path):
    json_paths = json_files(input_path)
    results = combine_results(json_paths)

    with open(output_path, "w") as file:
        json.dump(results, file)
    pass

if __name__ == "__main__":

    base_path = "D:/exp_runs/full"

    # input_path = os.path.join(base_path, "exps")
    # output_path = os.path.join(base_path, "experiments.txt")
    # extract_and_save(input_path, output_path)

    experiments_path = os.path.join(base_path, "experiments.txt")
    picture_path = os.path.join(base_path, "pictures")
    with open(experiments_path, "r") as file:
        experiments = json.load(file)

    # visualize(experiments, picture_path)

    visualize_migaheft(experiments, picture_path)

    pass
