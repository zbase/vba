import os, sys
import time, socket
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)
from migrationManager import *

class xx():
    def send_message(self, a):
        print "got for vbshandler",a

obj1 = {'Data': [{"Source": "10.36.162.24:11211", "Destination":"", "VbId":[0,1]}]}
obj = {'Data': [{"Source": "10.36.162.24:11211", "Destination":"10.32.162.25", "VbId":[],'Transfer_VbId':[12]}]}

mmPipe_r, mmPipe_w = socket.socketpair() 
q = xx()
mm = MigrationManager(q, mmPipe_r, mmPipe_w)
mm.run()
mmPipe_w.send("fff")
mm.set_config(obj)
time.sleep(200)
mmPipe_w.send("fff")
mm.set_config(obj1)
time.sleep(20)
