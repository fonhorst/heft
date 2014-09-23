import functools
import os
import shutil
from heft.settings import TEMP_PATH


def generate_pathes(dir):
        seq = (generate_pathes(os.path.join(dir, entry)) for entry in os.listdir(dir) if os.path.isdir(os.path.join(dir, entry)))
        result = functools.reduce(lambda x, y: x + y, seq, [dir])
        return result


def copy_to_target(path, output_path):
    files = [os.path.join(path, p) for p in os.listdir(path) if p.endswith(".json")]
    for p in files:
        out = os.path.join(output_path, os.path.basename(p))
        shutil.copyfile(p, out)
    pass


def union_pathes(output_path, *pathes):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    for path in pathes:
        if not os.path.isdir(path):
            raise ValueError("Path is not a directory: {0}".format(path))
        pths = generate_pathes(path)
        for p in pths:
            copy_to_target(p, output_path)
            pass
        pass
    pass

if __name__ == "__main__":
    output_path = os.path.join(TEMP_PATH, "compilation", "gaheft_for_gsa_m50_m75", "all")
    pathes = [os.path.join(TEMP_PATH, "compilation", "gaheft_for_gsa_m50_m75")]
    union_pathes(output_path, *pathes)