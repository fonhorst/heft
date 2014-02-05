from core.comparisons.VersusFunctors import GAvsHeftGA

reliability = 0.95
print("reliability %s" % reliability)
exp = GAvsHeftGA(reliability)

wf_names = ["Epigenomics_24"]


[exp(wf_name) for wf_name in wf_names]





