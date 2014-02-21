from datetime import datetime
from GA.DEAPGA.GAImplementation.GAImplementation import build, Params

# wf_names = ['small_bad']
# wf_names = ['CyberShake_50']
# wf_names = ['Sipht_30']
from core.comparisons.ComparisonBase import ComparisonUtility

wf_names = ['Montage_50']
# wf_names = ['Sipht_60']
# wf_names = ["Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100"]
# wf_names = ["Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100"]
# wf_names = ["Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]
# wf_names = ["Montage_25", "Montage_50", "Montage_75", "Montage_100"]
# wf_names = ['CyberShake_30']
# wf_names = ['CyberShake_50_sweep']
# wf_names = ['CyberShake_75', 'Montage_75', 'Epigenomics_72', 'Inspiral_72', 'Sipht_73']

# wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
#             "Montage_25", "Montage_50", "Montage_75", "Montage_100",
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]

if __name__ == '__main__':
    # start = ComparisonUtility.cur_time()
    for i in range(10):
        [build(wf_name, False, False, Params(20, 100, 0.8, 0.5, 0.4, 100), [10, 15, 25, 30]) for wf_name in wf_names]
    # end = ComparisonUtility.cur_time()
    #
    # print("start: " + str(start))
    # print("end: " + str(end))
# [build(wf_name, True, Params(20, 400, 0.9, 0.5, 0.8, 150), [10, 15, 25, 30, 45]) for wf_name in wf_names]
