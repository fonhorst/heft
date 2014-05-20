import functools
from scoop import futures
from GA.DEAPGA.GAImplementation.GARunner import MixRunner

wf_names = ['Montage_25']
# wf_names = ['Montage_500']
# wf_names = ['CyberShake_100']
# wf_names = ['Epigenomics_100']
# wf_names = ["CyberShake_50"]

PARAMS = {
    "ideal_flops": 20,
    "is_silent": True,
    "is_visualized": False,
    "ga_params": {
        "population": 200,
        "crossover_probability": 0.8,
        "replacing_mutation_probability": 0.5,
        "sweep_mutation_probability": 0.4,
        "generations": 300
    },
    "nodes_conf": [10, 15, 25, 30]
}

if __name__ == '__main__':
    print("Population size: " + str(PARAMS["ga_params"]["population"]))
    run = functools.partial(MixRunner(), **PARAMS)

    res = [run(wf_name) for wf_name in wf_names for i in range(1000)]

    print("RESULT: " + str([gam for gam, hm in res]))

    ## TODO: remove it later
    ## pop size equal to 200 is determined by the fact that cga uses 200 constructing of solutions
    ## So, We takes the same size for ga pop to be honest.
    ## TODO: need to implement ga operators as exacly as in "Science in the Cloud: Allocation and Execution of Data-Intesive Scientific Workflows."
    with open("D:/wspace/heft/temp", "w") as f:
        f.write(str(res))
    # list(futures.map(run, wf_names))
    # for i in range(5):
    #     print("ITERATION: " + str(i))
    #     run(wf_names[0])
    #     pass

