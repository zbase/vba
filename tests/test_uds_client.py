import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)

from mbMigratorHandler import *

name="/var/tmp/vbs/vbm.sock.10.36.175.180"
as_mgr = AsynCon()
params={"addr":name, "name":name, "timeout":10, "mgr":as_mgr}
m=MBMigratorHandler(params)
as_mgr.loop()
