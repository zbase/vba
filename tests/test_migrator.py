import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)
from asyncon import *
from migrator import *

class MB:
    def get(self, param):
        return {"source": "127.0.0.1:11211", "destination":"10.36.175.180:11211", "vblist":[0,1], "interface":""}

mb = MB()
as_mgr = AsynCon()
m=Migrator("key", mb, None, as_mgr)
as_mgr.loop()
