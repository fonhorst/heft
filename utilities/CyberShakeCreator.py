import os
import subprocess

wf_type = "MONTAGE"
wf_name = "Montage"

wd = "C:\\Users\\nikolay\\Downloads\\BharathiPaper (1).tar\\BharathiPaper (1)\\BharathiPaper"
cur_wd = os.getcwd()
os.chdir(wd)
path_dir = cur_wd + "\\..\\resources\\generated\\"

if not os.path.exists(path_dir):
    os.makedirs(path_dir)

# seq = range(250, 510, 10)
seq = [1]
for n in seq:
    path = path_dir + wf_name + "_" + str(n) + ".xml"
    out = open(path, "w")
    p = subprocess.Popen("java -jar WorkflowGenerator.jar -a {0} -- -n {1}".format(wf_type, n), stdout=out)
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
