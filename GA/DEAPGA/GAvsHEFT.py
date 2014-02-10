from GA.DEAPGA.GAImplementation import build

wf_names = ['CyberShake_100']
# wf_names = ['CyberShake_75', 'Montage_75', 'Epigenomics_72', 'Inspiral_72', 'Sipht_73']

# wf_names = ["CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100",
#             "Montage_25", "Montage_50", "Montage_75", "Montage_100",
#             "Epigenomics_24", "Epigenomics_46", "Epigenomics_72", "Epigenomics_100",
#             "Inspiral_30", "Inspiral_50", "Inspiral_72", "Inspiral_100",
#             "Sipht_30", "Sipht_60", "Sipht_73", "Sipht_100"]
[build(wf_name, True) for wf_name in wf_names]
