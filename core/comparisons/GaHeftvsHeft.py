from core.comparisons.ComparisonBase import ResultSaver, ComparisonUtility
from core.comparisons.VersusFunctors import GaHeftvsHeft

reliability = 0.95

save_file_name = ComparisonUtility.build_save_path('GaHeftvsHeft')
result_saver = ResultSaver(save_file_name)
exp = GaHeftvsHeft(reliability)
def calc(wf_name):
    return result_saver(exp(wf_name))

print("reliability %s" % reliability)

wf_names = ["Montage_25"]

[calc(wf_name) for wf_name in wf_names]
