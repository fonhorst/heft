import os
import subprocess

wf_type = "CYBERSHAKE"
wf_name = "CyberShake"

wd = "C:\\Users\\nikolay\\Downloads\\BharathiPaper (1).tar\\BharathiPaper (1)\\BharathiPaper"
cur_wd = os.getcwd()
os.chdir(wd)
path_dir = cur_wd + "\\..\\resources\\generated\\"

if not os.path.exists(path_dir):
    os.makedirs(path_dir)

for n in range(30, 110, 10):
    path = path_dir + wf_name + "_" + str(n) + ".xml"
    out = open(path, "w")
    p = subprocess.Popen("java -jar WorkflowGenerator.jar -a {0} -- -n {1}".format(wf_type, 30), stdout=out)
    p.communicate()
    out.close()
    out = open(path, "r")
    res = out.read().replace(",", ".")
    out.close()
    out = open(path, "w")
    out.write(res)
    out.close()


# for n in range(30, 110, 10):
#     (child_stdin, child_stdout) = os.popen2("java -jar WorkflowGenerator.jar -a {0} -- -n {1}".format(wf_type, n))
