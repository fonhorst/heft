import functools
import json
import os
from GA.DEAPGA.GAImplementation.GARunner import MixRunner
from experiments.cga.cga_exp import repeat
from experiments.cga.utilities.common import UniqueNameSaver

wf_names = ['Montage_25']
# wf_names = ['Montage_500']
# wf_names = ['CyberShake_100']
# wf_names = ['Epigenomics_100']
# wf_names = ["CyberShake_50"]

PARAMS = {
    "ideal_flops": 10,
    "is_silent": True,
    "is_visualized": False,
    "ga_params": {
        "population": 200,
        "crossover_probability": 0.8,
        "replacing_mutation_probability": 0.5,
        "sweep_mutation_probability": 0.4,
        "generations": 300
    },
    "nodes_conf": [10, 15, 25, 30],
    "transfer_time": 20
}

run = functools.partial(MixRunner(), **PARAMS)
directory = "../../temp/ga_vs_heft_exp"
saver = UniqueNameSaver("../../temp/ga_vs_heft_exp")

def do_exp():
    ga_makespan, heft_makespan = run(wf_names[0])
    saver(ga_makespan)
    return ga_makespan


if __name__ == '__main__':
    print("Population size: " + str(PARAMS["ga_params"]["population"]))

    repeat(do_exp, 2)

    result = []
    for entry in os.listdir(directory):
        p = os.path.join(directory, entry)
        if os.path.isfile(p):
            with open(p, "r") as f:
                a = float(f.read())
                result.append(a)
                pass
            os.remove(p)
        pass

    dt = {
        "metainfo": PARAMS,
        "results": result
    }

    with open(os.path.join(directory, "all_results.json"), "w") as f:
        json.dump(dt, f)





    #res = [run(wf_name) for wf_name in wf_names for i in range(1)]
    #print("RESULT: " + str([gam for gam, hm in res]))

    ## TODO: remove it later
    ## pop size equal to 200 is determined by the fact that cga uses 200 constructing of solutions
    ## So, We takes the same size for ga pop to be honest.
    ## TODO: need to implement ga operators as exacly as in "Science in the Cloud: Allocation and Execution of Data-Intesive Scientific Workflows."
    # with open("D:/wspace/heft/temp/ga_results.txt", "w") as f:
    #     f.write(str(res))
    # list(futures.map(run, wf_names))
    # for i in range(5):
    #     print("ITERATION: " + str(i))
    #     run(wf_names[0])
    #     pass

