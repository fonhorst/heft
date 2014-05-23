import json
from experiments.cga.utilities.common import generate_pathes


def aggr_ga():
    path = "../../../temp/ga_vs_heft_exp_10transfer_200pop/all_results.json"
    with open(path, "r") as f:
        data = json.load(f)
    print("Result: " + str(sum(data["results"])/len(data["results"])))

def aggr_ga_from_array():
    path = "../../../temp/ga_res_200popsize.txt"
    with open(path, "r") as f:
        data = json.load(f)
    print("Result: " + str(sum(data)/len(data)))

def aggr_cga():
    path = "../../../temp/cga_exp_200interact_50popsize_transfer_10/"
    pathes = generate_pathes(path)
    result = []
    for p in pathes:
        with open(p, "r") as f:
            data = json.load(f)
        result.append(data["final_makespan"])
    print(sum(result)/len(result))


if __name__ == "__main__":
    # aggr_cga()
    # aggr_ga()
    aggr_ga_from_array()