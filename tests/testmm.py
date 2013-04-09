import os, sys
import time
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PARENT_DIR)
from migrationManager import *

obj = {'Data': [{"Source": "10.36.162.24:11211", "Destination":"10.36.175.180:11211", "VbId":[0,1]}]}

mm = MigrationManager(None)
mm.run()
mm.set_config(obj)
time.sleep(50)
