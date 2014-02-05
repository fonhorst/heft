from core.comparisons.VersusFunctors import GAvsHeftGAvsHeftReXGA

reliability = 0.95
print("reliability %s" % reliability)
exp = GAvsHeftGAvsHeftReXGA(reliability)
wf_names = ['CyberShake_30']
##TODO: validity check fails here. This wf have something special in its structure. Need to look deeper.
#wf_names = ['Inspiral_72']

[exp(wf_name) for wf_name in wf_names]





