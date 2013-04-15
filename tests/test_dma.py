import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)
from asyncon import *
from diskMonitor import *

class TVBM:
    def set_kvstores(self, kvstores):
        print "Setting kvstores: %s" %kvstores

    def send_error(self, msg):
        print "Error message to VBS: %s" %msg

    def get_kv_stats(self, host):
        return None

    def get_vb_stats(self, host):
        return None

as_mgr = AsynCon()
vbs_mgr = TVBM()

d=DiskMonitor(vbs_mgr, as_mgr)
d.get_kvstores()
d.report_kvstores([3])
#as_mgr.loop()

