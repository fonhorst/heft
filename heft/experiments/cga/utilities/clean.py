import os

from heft.experiments.cga.utilities.common import generate_pathes


def clean_dir(directory):
    jsons = generate_pathes(directory)
    for j in jsons:
        os.remove(j)
    pass

if __name__ == "__main__":
    clean_dir("D:/FTP/cga_max_aggr")

