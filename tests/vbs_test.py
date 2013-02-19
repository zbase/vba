import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(PARENT_DIR))

from logger import *
from asyncon import *
from vbsHandler import *

as_mgr = AsynCon()
vbs_port = 6666
vbs_host = "127.0.0.1"

def handle_vbs_response(con, obj):
    Log.debug("Got response from vbs: %s" %obj)

if __name__ == '__main__':
    params = {"ip":vbs_host, "port":vbs_port, "callback":handle_vbs_response, 'mgr':as_mgr, "alive_msg": HEARTBEAT_CMD}
    vbsCon = VBSHandler(params)
    try:
        Log.info("Starting VBA")
        as_mgr.loop()
        Log.info("VBA Exitting")
    except Exception, e:
        Log.error("Exiting due to Exception %s" %why)
        os._exit(0)
