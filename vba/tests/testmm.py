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
import time, socket, time
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)
from migrationManager import *

class xx():
    def send_message(self, a):
        print "got for vbshandler",a

obj1 = {'Data': [{"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[0,1]}]}
obj = {'Data': [{"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[],'Transfer_VbId':[12]}, {"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[],'Transfer_VbId':[12]}, {"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[],'Transfer_VbId':[12]},{"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[],'Transfer_VbId':[12]},{"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[],'Transfer_VbId':[12]}]}

mmPipe_r, mmPipe_w = socket.socketpair() 
q = xx()
mm = MigrationManager(q, mmPipe_r, mmPipe_w)
mm.run()
mm.set_config(obj)
mmPipe_w.send("fff")
time.sleep(1)
mm.set_config(obj1)
mmPipe_w.send("fff")
time.sleep(30)
