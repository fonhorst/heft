from heft.core.CommonComponents.ExperimentalManagers import ExperimentResourceManager, ExperimentEstimator
from heft.core.environment import ResourceGenerator as rg
from heft.core.environment.Utility import wf
from heft.experiments.cga.utilities.common import extract_mapping_from_ga_file, extract_ordering_from_ga_file, UniqueNameSaver, repeat
from heft.experiments.cga.utilities.double_chromosome_ga import run_dcga
from heft.settings import __root_path__

_wf = wf("Montage_100")
rm = ExperimentResourceManager(rg.r([10, 15, 25, 30]))
estimator = ExperimentEstimator(None, ideal_flops=20, transfer_time=100)


heft_mapping = extract_mapping_from_ga_file("{0}/temp/heft_etalon_full_tr100_m100.json".format(__root_path__), rm)
heft_ordering = extract_ordering_from_ga_file("{0}/temp/heft_etalon_full_tr100_m100.json".format(__root_path__))

params = {
    "cxpb": 0.9,
    "mutpb": 0.9,
    "ngen": 100,
    "pop_size": 100
}

def do_exp():
    result, logbook = run_dcga(_wf, estimator, rm, heft_mapping, heft_ordering, **params)
    saver = UniqueNameSaver("{0}/temp/dcga_exp".format(__root_path__))
    data = {
        "final_makespan": result,
        "iterations": logbook
    }
    saver(data)
    return result

if __name__ == "__main__":
    res = repeat(do_exp, 1)
    print("RESULTS: ")
    print(res)