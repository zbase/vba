import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(PARENT_DIR))

from logger import *
from asyncon import *
from membaseHandler import *

as_mgr = AsynCon()
mb_ip = "127.0.0.1"
mb_port = "11211"
bind_ip = "127.0.0.1"

def handle_hbm_response(con, obj):
    Log.debug("Got response from vbs: %s" %obj)

if __name__ == '__main__':
    params = {"ip":mb_ip, "port":mb_port, "failCallback":hbmFailCallback, 'mgr':as_mgr, "bindIp":bind_ip, "timeout":10, "type":MembaseHandler.STATS_COMMAND}
    hbmCon = MembaseHandler(params)
    try:
        Log.info("Starting VBA")
        hbmCon.send_stats()
        as_mgr.loop()
        Log.info("VBA Exitting")
    except Exception, e:
        Log.error("Exiting due to Exception %s" %why)
        os._exit(0)
