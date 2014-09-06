from functools import partial
import os
from heft.experiments.aggregate_utilities import aggregate, WFS_COLORS
from heft.experiments.comparison_experiments.gavsheftmulti.aggregator import plot_aggregate_results
from heft.settings import TEMP_PATH


PATH = os.path.join(TEMP_PATH, "coeff_cyber_epig_100runs")

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

coeff_plot = partial(plot_aggregate_results, property_name="data_intensive_coeff", wfs_colors=WFS_COLORS)
coeff_aggregate = partial(aggregate, extract_and_add=extract_and_add_coeff, functions=[coeff_plot])

if __name__ == "__main__":

    # coeff_aggregate(PATH, "coeff.png")
    # wfs = [(WFS_COLORS_30, "30-series"), (WFS_COLORS_50, "50-series"),
    #        (WFS_COLORS_75, "75-series"), (WFS_COLORS_100, "100-series")]

    wfs = [#({"Montage_25": "-gD"}, "montage"),
            ({"CyberShake_30": "-rD"}, "cybershake"),
            #({"Inspiral_30": "-bD"}, "inspiral"),
            #({"Sipht_30": "-yD"}, "sipht"),
            ({"Epigenomics_24": "-mD"}, "epigenomics")]
    for wfs_colors, name in wfs:
        f = partial(plot_aggregate_results, property_name="data_intensive_coeff", wfs_colors=wfs_colors)
        aggregate(PATH,"coeff_{0}.png".format(name), extract_and_add=extract_and_add_coeff, functions=[f])