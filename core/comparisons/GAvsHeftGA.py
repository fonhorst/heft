from core.comparisons.ComparisonBase import ResultSaver, ComparisonUtility
from core.comparisons.VersusFunctors import GAvsHeftGA

reliability = 0.99

save_file_name = ComparisonUtility.build_save_path('CloudHeftvsHeft')
result_saver = ResultSaver(save_file_name)
exp = GAvsHeftGA(reliability)
def calc(wf_name):
    return result_saver(exp(wf_name))

print("reliability %s" % reliability)

wf_names = ["Montage_50"]

result = [calc(wf_name) for wf_name in wf_names]

print(str(result))

# result = [calc(wf_names[0]) for i in range(10)]

# avrs = [r['algorithms']["ga"]['Avr'] for r in result]

# print("Result: " + str(sum(avrs)/len(avrs)))




