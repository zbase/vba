#!/usr/bin/env python
import os
import sys
import migrationManager
import vbsManager
import mbManager
import asyncon
import getopt
import diskMonitor
import socket
from logger import *

#VBA_PID_FILE = "/var/run/vbs/vba.pid"
VBA_PID_FILE = "/tmp/vba.pid"
DEFAULT_VBS_HOST = "127.0.0.1"
DEFAULT_VBS_PORT = 14000

Log = getLogger()
vbs_host = DEFAULT_VBS_HOST
vbs_port = DEFAULT_VBS_PORT

def parse_options(opts):
    global vbs_host, vbs_port
    for o,a in opts:
        if (o == "-f"):
            file = open(a) 
            for line in file:
                line = line.strip('\n')
                if (line.find(':') != -1):
                    vbs_host,vbs_port = line.split(":")
                    if (vbs_host == ''):
                        vbs_host = DEFAULT_VBS_HOST
                    if (vbs_port == ''):
                        vbs_port = DEFAULT_VBS_PORT
                    else:
                        vbs_port = int(vbs_port)
                elif (line != ''):
                    vbs_host = line
                    vbs_port = DEFAULT_VBS_PORT
            file.close()

def getPipes():
    mmPipe_r, mmPipe_w = socket.socketpair() 
    mbPipe_r, mbPipe_w = socket.socketpair() 
    return mmPipe_r, mmPipe_w, mbPipe_r, mbPipe_w 


if __name__ == '__main__':
    mypid = os.getpid() 
    pidfile = open(VBA_PID_FILE, 'w')
    pidfile.write(str(mypid))
    pidfile.close()

    opts, args = getopt.getopt(sys.argv[1:], 'f:')
    if (len(opts) != 0):
        parse_options(opts)
    vbs_port = int(vbs_port)

    mmPipe_r, mmPipe_w, mbPipe_r, mbPipe_w = getPipes()
    as_mgr = asyncon.AsynCon()
    vbs_manager = vbsManager.VBSManager(vbs_host, vbs_port, mmPipe_w, mbPipe_w, as_mgr)
    migration_mgr = migrationManager.MigrationManager(vbs_manager, mmPipe_r, mmPipe_w)
    mb_mgr = mbManager.MembaseManager(vbs_manager, mbPipe_r)

    #Set the managers in vbsManager for callbacks
    vbs_manager.set_migration_manager(migration_mgr)
    vbs_manager.set_membase_manager(mb_mgr)

    #Membase manager starts its own thread
    mb_mgr.run()
    #Migration manager starts its own thread
    migration_mgr.run()
    dm = diskMonitor.DiskMonitor(vbs_manager, as_mgr)
    kvstores = dm.get_kvstores()
    Log.debug("Got KVStores: %s" %kvstores)

    #Async loop for VBS manager
    try:
        as_mgr.loop()
    except Exception, e:
        Log.critical("Exiting vba %s" %e)
