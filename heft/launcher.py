"""
this launcher can start scripts as a main point of entering
which lay inside library hierarchy without adding it to pythonpath
but working directory must be the same where this scipt lays
(and root package 'heft' must lay there too)

NOTE: Symlinks met along path may lead to exceptions
"""
import os
import sys
from importlib import import_module

if __name__ == "__main__":
    #for arg in sys.argv:
     #   print(arg)
    assert len(sys.argv) > 1, "qualified name for script hasn't been given"
    assert os.path.exists(sys.argv[-1]), "Path is not valid"

    script_name = sys.argv[-1]
    cur_dir = os.getcwd()

    assert script_name.startswith(cur_dir), "script doesn't lay in the directory hierarchy of the project"


    script_name = script_name[len(cur_dir):]
    script_name = script_name.replace("/", ".").strip(".")
    script_name = script_name[0:-3] if script_name.endswith(".py") else script_name
    exp = import_module(script_name)
    exp.do_exp()