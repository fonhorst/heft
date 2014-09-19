import json
import os
import matplotlib.pyplot as plt
from heft.settings import TEMP_PATH

WFS_COLORS_30 = {
    # 30 - series
    "Montage_25": "-gD",
    "CyberShake_30": "-rD",
    "Inspiral_30": "-bD",
    "Sipht_30": "-yD",
    "Epigenomics_24": "-mD",
}

WFS_COLORS_50 = {
    # 50 - series
    "Montage_50": "-gD",
    "CyberShake_50": "-rD",
    "Inspiral_50": "-bD",
    "Sipht_60": "-yD",
    "Epigenomics_46": "-mD",
}


WFS_COLORS_75 = {
    # 75 - series
    "Montage_75": "-gD",
    "CyberShake_75": "-rD",
    "Inspiral_72": "-bD",
    "Sipht_73": "-yD",
    "Epigenomics_72": "-mD",
}


WFS_COLORS_100 = {
    # 100 - series
    "Montage_100": "-gD",
    "CyberShake_100": "-rD",
    "Inspiral_100": "-bD",
    "Sipht_100": "-yD",
    "Epigenomics_100": "-mD",
}

WFS_COLORS = dict()
WFS_COLORS.update(WFS_COLORS_30)
WFS_COLORS.update(WFS_COLORS_50)
WFS_COLORS.update(WFS_COLORS_75)
WFS_COLORS.update(WFS_COLORS_100)


def visualize(data, functions, path_to_save=None):
    plt.figure(figsize=(10, 10))

    for i in range(len(functions)):
        plt.subplot(len(functions), 1, i + 1)
        functions[i](data)

    plt.tight_layout()

    if path_to_save is None:
        plt.show()
    else:
        plt.savefig(path_to_save, dpi=96.0, format="png")
        plt.clf()
    pass


def aggregate(pathes,  picture_path="gh.png", extract_and_add=None, functions=None):
    files = [os.path.join(path, p) for path in pathes for p in os.listdir(path) if p.endswith(".json")]
    data = {}
    for p in files:
        with open(p, "r") as f:
            d = json.load(f)
            extract_and_add(data, d)

    path = os.path.join(TEMP_PATH, picture_path) if not os.path.isabs(picture_path) else picture_path
    visualize(data, functions, path)
