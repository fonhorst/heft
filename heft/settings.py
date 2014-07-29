import os

"""
This module is intended to store all global project settings
"""

__root_path__ = os.path.dirname(os.path.dirname(__file__))
RESOURCES_PATH = os.path.join(__root_path__, "resources")
TEMP_PATH = os.path.join(__root_path__, "temp")
