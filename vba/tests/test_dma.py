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

