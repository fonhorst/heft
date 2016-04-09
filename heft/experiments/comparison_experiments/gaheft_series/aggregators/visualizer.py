from itertools import groupby
import json
import os
from statistics import mean


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
                    "reliability_value": "value",
                    "mean_value": "value",
                    "values_set": [("fail_count", "value")]
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
            #"values": values_array,
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


if __name__ == "__main__":

    output_path = "D:/exp_runs/experiments.txt"

    json_paths = json_files("D:/exp_runs/all")
    results = combine_results(json_paths)

    with open(output_path, "w") as file:
        json.dump(results, file)

    pass
