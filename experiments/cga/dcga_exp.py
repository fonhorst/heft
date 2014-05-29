from core.concrete_realization import ExperimentResourceManager, ExperimentEstimator
from experiments.cga import wf
from experiments.cga.utilities.common import extract_mapping_from_ga_file, extract_ordering_from_ga_file, UniqueNameSaver, repeat
from environment.ResourceGenerator import ResourceGenerator as rg
from experiments.cga.utilities.double_chromosome_ga import run_dcga

_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)


heft_mapping = extract_mapping_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json", rm)
heft_ordering = extract_ordering_from_ga_file("../../temp/heft_etalon_full_tr100_m100.json")

params = {
    "cxpb": 0.9,
    "mutpb": 0.9,
    "ngen": 100,
    "pop_size": 100
}

def do_exp():
    result = run_dcga(_wf, estimator, rm, heft_mapping, heft_ordering, **params)
    saver = UniqueNameSaver("../../temp/dcga_exp")
    data = {
        "final_makespan": result
    }
    saver(data)
    return result

if __name__ == "__main__":
    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)