import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)
print "Importing from %s" %PARENT_DIR

from logger import *
from asyncon import *
from message import *
from vbsManager import *

as_mgr = AsynCon()
vbs_port = 14000
vbs_host = "127.0.0.1"
HEARTBEAT_CMD = VBSMessage.getMsg('{"Cmd":"ALIVE"}')

if __name__ == '__main__':
    vbsManager = VBSManager(vbs_host, vbs_port, as_mgr)
    while True:
        try:
            print("Starting VBA")
            as_mgr.loop()
            print "VBA async loop exit"
        except Exception, why:
            Log.error("Exiting due to Exception %s" %why)
            os._exit(0)
