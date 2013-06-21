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
from migrator import *

class MB:
    def get(self, param):
        return {"source": "127.0.0.1:11211", "destination":"10.36.175.180:11211", "vblist":[0,1], "interface":""}

mb = MB()
as_mgr = AsynCon()
m=Migrator("key", mb, None, as_mgr)
as_mgr.loop()
