import os
from uuid import uuid4

from heft.experiments.comparison_experiments.common.ComparisonBase import ResultSaver, ComparisonUtility
from heft.experiments.comparison_experiments.common.VersusFunctors import GaHeftvsHeft


reliability = 0.99

wf_name = "Montage_25"

save_file_name = ComparisonUtility.build_save_path(wf_name + '\\GaHeftvsHeft_['+str(uuid4())+']')
result_saver = ResultSaver(save_file_name)
exp = GaHeftvsHeft(reliability, n=1)
def calc(wf_name, out):
    return result_saver(exp(wf_name, out))

print("fail_duration: 40")
print("reliability %s" % reliability)

base_dir = "../../resources/experiment_1/"
if not os.path.exists(base_dir):
    os.makedirs(base_dir)
output_file_template = base_dir + "[{0}]_[{1}]_[{2}].txt"
out = lambda w_name: output_file_template.format(w_name, reliability, ComparisonUtility.cur_time())

wf_names = [wf_name]

[calc(wf_name, out(wf_name)) for wf_name in wf_names]
