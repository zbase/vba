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
