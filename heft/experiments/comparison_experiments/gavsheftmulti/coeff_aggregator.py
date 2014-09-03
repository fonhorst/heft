from functools import partial
import json
import os
from heft.experiments.comparison_experiments.gavsheftmulti.aggregator import plot_aggregate_results, visualize
from heft.settings import TEMP_PATH

coeff_plot = partial(plot_aggregate_results, property_name="data_intensive_coeff")
PATH = os.path.join(TEMP_PATH, "ga_vs_heft_multi_coeff_test_2_eb7c8a24-8593-47d3-9eb3-471c103d0314")

def extract_and_add_coeff(data, d):
    """
    Intermediate data representation for analysis
    {
        "{wf_name}": {
            "data_intensive_coeff": {
                "{value}" : []
                "transfer_time": ..
            }
        }
    }
    """
    wf_name = d["wf_name"]
    data_intensive_coeff = d["params"]["data_intensive_coeff"]
    heft_makespan = d["heft"]["makespan"]
    ga_makespan = d["ga"]["makespan"]


    default_entry = {"data_intensive_coeff": {}}
    el = data.get(wf_name, default_entry)

    ## check for experiments with {fix_flops, fix_transfer} configuration
    ## to eliminate results match pattern {fix_flops, ...}

    coeff_results = el["data_intensive_coeff"].get(data_intensive_coeff, [])
    coeff_results.append({"h": heft_makespan, "g": ga_makespan})
    el["data_intensive_coeff"][data_intensive_coeff] = coeff_results

    data[wf_name] = el

    return data


def aggregate(path, picture_path="gh.png"):
    files = [os.path.join(path, p) for p in os.listdir(path) if p.endswith(".json")]
    data = {}
    for p in files:
        with open(p, "r") as f:
            d = json.load(f)
            extract_and_add_coeff(data, d)

    path = os.path.join(TEMP_PATH, picture_path) if not os.path.isabs(picture_path) else picture_path
    visualize(data, [coeff_plot], path)

if __name__ == "__main__":
    aggregate(PATH)
