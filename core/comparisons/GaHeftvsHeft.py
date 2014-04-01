import os
from core.comparisons.ComparisonBase import ResultSaver, ComparisonUtility
from core.comparisons.VersusFunctors import GaHeftvsHeft

reliability = 0.99

save_file_name = ComparisonUtility.build_save_path('GaHeftvsHeft')
result_saver = ResultSaver(save_file_name)
exp = GaHeftvsHeft(reliability, n=10)
def calc(wf_name, out):
    return result_saver(exp(wf_name, out))

print("reliability %s" % reliability)

base_dir = "../../resources/experiment_1/"
if not os.path.exists(base_dir):
    os.makedirs(base_dir)
output_file_template = base_dir + "[{0}]_[{1}]_[{2}].txt"
out = lambda w_name: output_file_template.format(w_name, reliability, ComparisonUtility.cur_time())

wf_names = ["Montage_25"]

[calc(wf_name, out(wf_name)) for wf_name in wf_names]
