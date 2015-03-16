import json
import os
import shutil
from heft.settings import TEMP_PATH

__author__ = 'adminuser'

def pack_wf(path, output_path,  wf_name):
    files = [os.path.join(path, p) for p in os.listdir(path) if p.endswith(".json")]
    for p in files:
        with open(p, "r") as f:
            d = json.load(f)
        if d["wf_name"] == wf_name:
            out = os.path.join(output_path, os.path.basename(p))
            shutil.copyfile(p, out)
    pass

if __name__ == "__main__":
    path = os.path.join(TEMP_PATH, "coeff_exp")
    output_path = os.path.join(TEMP_PATH, "epigenomics")
    wf_name = "Epigenomics_24"
    pack_wf(path, output_path, wf_name)
