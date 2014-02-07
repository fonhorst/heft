from core.comparisons.ComparisonBase import ResultSaver, ComparisonUtility
from core.comparisons.VersusFunctors import GAvsHeftGA

reliability = 0.95

save_file_name = ComparisonUtility.build_save_path('CloudHeftvsHeft')
result_saver = ResultSaver(save_file_name)
exp = GAvsHeftGA(reliability)
def calc(wf_name):
    return result_saver(exp(wf_name))

print("reliability %s" % reliability)

wf_names = ["Epigenomics_24"]


[calc(wf_name) for wf_name in wf_names]





