"""
Copyright 2013 Zynga Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os, sys
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)

from logger import *
from asyncon import *
from membaseHandler import *

as_mgr = AsynCon()
mb_ip = "127.0.0.1"
mb_port = 11211
bind_ip = "127.0.0.1"

class mbmgr:
    def report_stats(self, msg):
        print "Send to vbs: %s" %msg

def hbmFailCallback(obj):
    print "HBM failed for %s" %(obj.ip)
    obj.destroy()

def handle_hbm_response(con, obj):
    Log.debug("Got response from vbs: %s" %obj)

if __name__ == '__main__':
    mb_mgr = mbmgr()
    params = {"ip":mb_ip, "port":mb_port, "failCallback":hbmFailCallback, 'mgr':as_mgr, "bindIp":bind_ip, "timeout":10, "type":MembaseHandler.VB_STATS_MONITORING, 'mb_mgr':mb_mgr}
    hbmCon = MembaseHandler(params)
    try:
        print("Starting VBA")
        hbmCon.send_stats()
        as_mgr.loop()
        print("VBA Exitting")
    except Exception, why:
        print("Exiting due to Exception %s" %why)
        os._exit(0)

