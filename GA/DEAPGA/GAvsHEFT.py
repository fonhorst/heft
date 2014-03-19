from GA.DEAPGA.GAImplementation.GARunner import MixRunner

wf_names = ['Montage_25']
# wf_names = ['Montage_500']
# wf_names = ['CyberShake_100']
# wf_names = ['Epigenomics_100']
# wf_names = ["CyberShake_50"]

PARAMS = {
    "ideal_flops": 20,
    "is_silent": False,
    "is_visualized": False,
    "ga_params": {
        "population": 50,
        "crossover_probability": 0.8,
        "replacing_mutation_probability": 0.5,
        "sweep_mutation_probability": 0.4,
        "generations": 200
    },
    "nodes_conf": [10, 15, 25, 30]
}

if __name__ == '__main__':
    run = MixRunner()
    [run(wf_name, **PARAMS) for wf_name in wf_names]

