from core.comparisons.VersusFunctors import CloudHeftVsHeft

reliability = 0.95
print("reliability %s" % reliability)
exp = CloudHeftVsHeft(reliability)

wf_names = ["CyberShake_30"]

# wf_names = ['generated\\CyberShake_70']

# wf_names = ['generated\\CyberShake_30',
#             'generated\\CyberShake_40',
#             'generated\\CyberShake_50',
#             'generated\\CyberShake_60',
#             'generated\\CyberShake_65',
#             'generated\\CyberShake_70',
#             'generated\\CyberShake_75',
#             'generated\\CyberShake_80',
#             'generated\\CyberShake_90',
#             'generated\\CyberShake_100']

#wf_names = ['new_generated\\CyberShake_30','new_generated\\CyberShake_50','new_generated\\CyberShake_75','new_generated\\CyberShake_100']
# wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
#             "Montage_25", "Montage_50", "Montage_75", "Montage_100",
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]


[exp(wf_name) for wf_name in wf_names]















