import functools
import json
import os
import shutil
from heft.settings import TEMP_PATH
from heft.utilities.union_to_one_dir import PathesMerger


class ClassificationPathesMerger(PathesMerger):
    def copy_to_target(self, path, output):
        files = [os.path.join(path, p) for p in os.listdir(path) if p.endswith(".json")]
        for file in files:
            with open(file, "r") as f:
                data = json.load(f)

            experiment_name = data["params"]["experiment_name"]
            alg_name = data["params"]["alg_name"]
            reliability = data["params"]["estimator_settings"]["reliability"]
            pop_size = data["params"]["alg_params"]["n"]
            wf_name = data["wf_name"]

            dir_name = "[{0}]_[{1}]_[{2}]_[n-{3}]_[rel-{4}]".format(experiment_name, alg_name, wf_name, pop_size, reliability)
            file_name = os.path.basename(file)

            parent_path = os.path.join(output, dir_name)
            if not os.path.exists(parent_path):
                os.makedirs(parent_path)

            out_path = os.path.join(parent_path, file_name)
            shutil.copyfile(file, out_path)



if __name__ == "__main__":
    # base_path = "/opt/wspace/heft/temp/compilation/gaheft_results_not_sorted.zip__FILES/"
    # exps = {
    #     "ga": ["migaheft_for_pso_cc7f423f-d523-43cb-8375-327ab1fe5c2e/migaheft_for_ga_8a5ee048-970f-49c6-b08d-f674bbfa2fd9",
    #            "migaheft_for_pso_dd2cf684-c53c-4ebb-9469-22eb79141edb/migaheft_for_ga_31795753-0d7e-4de1-a035-2d6004d8551c",
    #            "migaheft_for_pso_dd2cf684-c53c-4ebb-9469-22eb79141edb/migaheft_for_ga_46857954-7796-4511-9cb7-c0c56e3e22d2"
    #            #"migaheft_for_pso_8d566310-5985-46a7-b44e-594436e21491/migaheft_for_ga_bed7be0e-5575-40a0-8ee6-f91a58a43017",
    #            # "migaheft_for_pso_929732bc-350c-4cd4-9e05-937d679ed2b7/migaheft_for_ga_5e25c333-70f9-4282-99f4-d7905cd8150b",
    #            # "migaheft_for_pso_cc7f423f-d523-43cb-8375-327ab1fe5c2e/migaheft_for_ga_8a5ee048-970f-49c6-b08d-f674bbfa2fd9",
    #            # "migaheft_for_pso_dd2cf684-c53c-4ebb-9469-22eb79141edb/migaheft_for_ga_31795753-0d7e-4de1-a035-2d6004d8551c",
    #           #  "migaheft_for_pso_dd2cf684-c53c-4ebb-9469-22eb79141edb/migaheft_for_ga_46857954-7796-4511-9cb7-c0c56e3e22d2"
    #     ],
    #
    #     #"gsa": ["migaheft_for_pso_192de8af-54b0-41e1-9b08-bfa98e597332/migaheft_for_gsa_f38bf33b-d9f7-4064-b18b-8bd73b4624f8",
    #    #         "migaheft_for_pso_481b344e-8356-43a2-b965-908f21c61a73/migaheft_for_gsa_43d15ee4-db89-4e13-9c38-a922677af7f6",
    #    #         "migaheft_for_pso_d05c8599-3776-4ddc-b0e9-bd5d2e6136b2/migaheft_for_gsa_1aa0bcc7-dd99-4e68-bfc5-1dc85b69ca20",
    #     #        "migaheft_for_pso_faccc170-58ec-431e-8001-07760cadb005/migaheft_for_gsa_88c33b71-049c-4244-bbb1-d4225f441b52"]
    # }
    #

    folder = os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_pso_[0.99-0.9]x[m25-m75]x50")
    output_folder = os.path.join(TEMP_PATH, "old/all_results_sorted_and_merged/gaheft_0.99-0.9_series/gaheft_for_pso_[m25,m40]_[0.95]")
    wf_names = ["Montage_25", "Montage_40"]

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file_name in os.listdir(folder):
        if file_name.endswith(".json"):
            path = os.path.join(folder, file_name)
            with open(path, "r") as f:
                data = json.load(f)
            if data["wf_name"] in wf_names and data["params"]["estimator_settings"]["reliability"] == 0.95:
                output = os.path.join(output_folder, file_name)
                shutil.copyfile(path, output)



    # output_path = os.path.join(TEMP_PATH, "compilation", "gaheft_results_sorted")
    # pathes = [os.path.join(TEMP_PATH, "compilation", "gaheft_results_not_sorted.zip__FILES")]
    #
    # merger = ClassificationPathesMerger()
    # merger(output_path, *pathes)
