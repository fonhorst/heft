from environment.Utility import Utility

def wf(wf_name):
    dax_filepath = "../../resources/{0}.xml".format(wf_name)
    _wf = Utility.readWorkflow(dax_filepath, wf_name)
    return _wf
