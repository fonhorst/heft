import os

path = "D:\eScience\Work\experiments\CWRPSO"

wf_list = ["Montage_25", "Montage_50", "Montage_75", "Montage_100", "CyberShake_30", "CyberShake_50", "CyberShake_75", "CyberShake_100"]

alg = "crdpso"

for wf in wf_list:

    full_path = os.path.join(path, (wf + "_" + alg + ".plt"))

    file = open(full_path, "w")

    script = "set terminal png enhanced font \'Verdana,14\'\n"
    script += "set output " + "\"" + wf + "_" + alg + ".png\"\n"

    script += "set title " + "\"" + wf.replace("_", "") + "\"\n"
    script += "set xlabel \"generation\"\n"
    script += "set ylabel \"Makespan, ms\"\n"
    script += "set grid ytics lc rgb \"#bbbbbb\" lw 1 lt 0\n"
    script += "set grid xtics lc rgb \"#bbbbbb\" lw 1 lt 0\n"
    script += "plot \'" + wf + "_" + alg + ".txt\' u 1:4 with lines lw 4 lt rgb \"green\" title \"" + alg + "\""

    file.write(script)




