#!/usr/bin/env python
import migrationManager
import vbsManager
import mbManager
import asyncon
from logger import *

Log = getLogger()

if __name__ == '__main__':

    mypid = os.getpid() 
    pidfile = open(VBA_PID_FILE, 'w')
    pidfile.write(str(mypid))
    pidfile.close()

    as_mgr = asyncon.AsynCon()
    vbsManager = vbsManager.VBSManager(vbs_host, vbs_port, as_mgr)
    migration_mgr = migrationManager.MigrationManager(vbsManager)
    mb_mgr = mbManager.MembaseManager(vbsManager)

    #Set the managers in vbsManager for callbacks
    vbsManager.setMigrationManager(migration_mgr)
    vbsManager.setMembaseManager(mb_mgr)

    #Migration manager starts its own thread
    migration_mgr.run()
    #Membase manager starts its own thread
    mb_manager.run()

    #Async loop for VBS manager
    try:
        as_mgr.loop()
    except Exception, e:
        Log.critical("Exiting vba %s" %e)

