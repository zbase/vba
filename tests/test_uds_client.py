import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)

from mbMigratorHandler import *

name="/tmp/test_uds"
as_mgr = AsynCon()
params={"addr":name, "name":"test_name", "timeout":10, "mgr":as_mgr}
m=MBMigratorHandler(params)
as_mgr.loop()
