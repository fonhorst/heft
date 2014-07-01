from config.settings import __root_path__
from core.environment import Utility


def wf(wf_name):
    # dax_filepath = "../../resources/{0}.xml".format(wf_name)
    dax_filepath = "{0}/resources/{1}.xml".format(__root_path__, wf_name)
    _wf = Utility.readWorkflow(dax_filepath, wf_name)
    return _wf
