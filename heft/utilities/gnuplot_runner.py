
import os

path = "D:\eScience\Work\experiments"

wf_list = ["Montage_25", "Montage_50", "Montage_75", "CyberShake_30", "CyberShake_50", "CyberShake_75"]

alg = "cgsa"

for wf in wf_list:
    os.system('gnuplot ' + os.path.join(path, (wf + "_" + alg)) + ".plt" + "")