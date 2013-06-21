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

class TimerTest(AsynConDispatcher):
    def __init__(self, mgr, id):
        self.timer = 2
        self.mgr = mgr
        self.id = id
        AsynConDispatcher.__init__(self, None, self.timer, self.mgr)
        self.create_timer()
        self.set_timer()
        self.count = 0
        self.max_count = 100

    def set_timer(self):
        self.timer_event.add(self.timer)

    def handle_timer(self):
        for i in range(100):
            j = i+1
        self.count += 1
        #print "ID: %d Count: %d" %(self.id, self.count)
        if self.count < self.max_count:
            self.set_timer()

as_mgr = AsynCon()

l = []
for i in range(65000):
    l.append(TimerTest(as_mgr,i))

as_mgr.loop()
