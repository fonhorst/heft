from experiments.comparison_experiments.utilities.ComparisonBase import ComparisonUtility, ResultSaver
from experiments.comparison_experiments.utilities.VersusFunctors import GAvsHeftGAvsHeftReXGA

reliability = 0.95

save_file_name = ComparisonUtility.build_save_path('CloudHeftvsHeft')
result_saver = ResultSaver(save_file_name)
exp = GAvsHeftGAvsHeftReXGA(reliability)
def calc(wf_name):
    return result_saver(exp(wf_name))
print("reliability %s" % reliability)
wf_names = ['CyberShake_30']
##TODO: validity check fails here. This wf have something special in its structure. Need to look deeper.
#wf_names = ['Inspiral_72']

[calc(wf_name) for wf_name in wf_names]





