import os
import sys

scripts_dir = os.path.join(os.path.dirname(__file__), 'scripts')
sys.path.append(scripts_dir)
# Do not compile .pyc files for the tool modules.
sys.dont_write_bytecode = True

from MultipleFloodRastersToShapefile import MultipleFloodRastersToShapefile

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "AutoRoute Tools"
        self.alias = "AutoRoute Tools"

        # List of tool classes associated with this toolbox
        self.tools = [MultipleFloodRastersToShapefile]

