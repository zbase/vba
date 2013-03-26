#!/usr/bin/env python
"""
VBucket agent
"""

import socket
import string
import struct
import time
import os
import signal
import sys
import threading 
import Queue
import simplejson as json
import subprocess
import zlib
import re
import getopt
import socket, IN
from logger import *
from asyncon import *
from message import *
from vbsManager import *
from mbManager import *

Log = getLogger()
INIT_CMD_STR = "INIT"
CONFIG_CMD_STR = "CONFIG"
HEARTBEAT_CMD_STR = "Alive"
DEFAULT_HEARTBEAT_INTERVAL = 120
DEFAULT_SLEEP = 10
DEFAULT_VBS_HOST = "127.0.0.1"
DEFAULT_VBS_PORT = 14000
DEFAULT_MB_HOST = "127.0.0.1"
DEFAULT_MB_PORT = 11211
BAD_DISK_FILE = "/var/tmp/vbs/bad_disk"
VBA_PID_FILE = "/var/run/vbs/vba.pid"

VBM_STATS_SOCK = "/var/tmp/vbs/vbm.sock"
MB_PID_FILE = "/var/run/memcached/memcached.pid"
DEFAULT_REPLICATION_DIFF = 1000

CHECKPOINT_STATS_STR = "stats checkpoint"
VBUCKET_STATS_STR = "stats vbucket"
KVSTORE_STATS_STR = "stats kvstore"

INVALID_CMD     = 0
INIT_CMD        = 1
CONFIG_CMD      = 2

MIN_DATA_LEN    = 4 # The first 4 bytes of a packet give us the length 

TAP_REGISTRATION_SCRIPT_PATH = "/opt/membase/lib/python/mbadm-tap-registration"
VBUCKET_MIGRATOR_PATH = "/opt/membase/bin/vbucketmigrator"
MBVBUCKETCTL_PATH = "/opt/membase/lib/python/mbvbucketctl"

UNKNOWN_CMD_JSON = '{"Cmd":"UNKNOWN", "Status":"ERROR"}'
HEARTBEAT_CMD = '{"Cmd":"ALIVE"}'



class MigrationManager:
    """Class to parse the config and manage the migrators (which run the VBMs)"""

    def __init__(self, server, port):
        self.vbs_server = server
        self.vbs_port = port
        self.rowid = 0
        self.vbtable = {}

    def create_migrator(self, key, row):
        migrator_obj = Migrator(key, self)
        row['migrator'] = migrator_obj
        return migrator_obj

    def end_migrator(self, key):
        migrator_obj = self.vbtable[key].get('migrator')
        if (migrator_obj != None):
            migrator_obj.join()

    def parse_config_row(self, row):
        source = row.get('Source')
        if (':' not in source):
            source = source + ':' + str(DEFAULT_MB_PORT)

        dest = row.get('Destination')
        if (dest != '' and ':' not in dest):
            dest = dest + ':' + str(DEFAULT_MB_PORT)

        vblist = row.get('VbId')

        if (source == '' or len(vblist) == 0):
            raise RuntimeError("For row [" + str(row) + "], source/vbucket list missing")
            
        vblist.sort()
        key = source + "|" + dest
        value = {}
        value['source'] = source
        value['destination'] = dest
        value['vblist'] = vblist
        return key, value

    def get_iface_for_ip(self, ip, ifaces):
        ret_iface = ''
        for iface in ifaces:
            if ip == iface[1]:
                ret_iface = iface[0]
                break
        return ret_iface

    def handle_new_config(self, config_cmd, ifaces):
        new_vb_table = {}
        global heartbeat_interval
        if ('HeartBeatTime' in config_cmd):
            heartbeat_interval = config_cmd['HeartBeatTime']

        config_data = config_cmd.get('Data')
        if (config_data == None or len(config_data) == 0):
            Log.warning('VBucket map missing in config')
            return json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":["No Vbucket map in config"]})

        Log.debug('New config from VBS: %s', str(config_data))

        # Create a new table(new_vb_table) from the config data
        # The config data is of the form:
        # [
        #   {"Source":"192.168.1.1:11211", "VbId":[1,2,3], "Destination":"192.168.1.2:11211"},  
        #   {"Source":"192.168.1.1:11211", "VbId":[7,8,9], "Destination":"192.168.1.5:11511"},  
        #   .
        #   .
        # ]

        # The table we maintain is of the form:
        #   source                destination           vblist      interface      migrator
        #   192.168.1.1:11211     192.168.1.2:11211     1,2,3       eth1          
        #   192.168.1.1:11211     192.168.1.5:11511     7,8,9       eth1          
        #   .
        #   .

        err_details = []
        for row in config_data:
            try:
                (key, value) = self.parse_config_row(row)
                (ip,port) = value['source'].split(':')
                value['interface'] = self.get_iface_for_ip(ip, ifaces)
                new_vb_table[key] = value
            except RuntimeError, (message):
                err_details.append(message)

        new_migrators = []
        # Compare old and new vb tables
        if (len(self.vbtable) == 0):    # First time, start VBMs for all rows in new_vb_table                            
            for (k, v) in new_vb_table.iteritems():
                new_migrators.append(self.create_migrator(k,v))
        else:
            # Iterate over the new table and:
            #   If the row is present in the old table 
            #       restart the VBM
            #   else 
            #       start up a new VBM
            for (k, v) in new_vb_table.iteritems():
                if k in self.vbtable and self.vbtable[k].get('migrator').isAlive():
                    if (self.vbtable[k]['vblist'] != v['vblist']):
                        # Kill the existing VBM and start a new one
                        Log.debug('Vbucket list changed for row [%s] from %s to %s, will restart the vbucket migrator', k, v['vblist'], self.vbtable[k]['vblist'])
                        self.end_migrator(k)
                        new_migrators.append(self.create_migrator(k,v))
                    else:
                        # just copy the migrator obj to the new table
                        v['migrator'] =  self.vbtable[k].get('migrator')
                else:
                    # Start a new VBM
                    Log.debug('Starting a new vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
                    new_migrators.append(self.create_migrator(k,v))

                # Iterate over the old table and:
                #   If the key is not found in the new table, kill the VBM for that row
                for (k, v) in self.vbtable.iteritems():
                    if k not in new_vb_table:
                        # Kill the VBM for the row
                        Log.debug('Killing vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
                        self.end_migrator(k)

        self.vbtable = new_vb_table
        # Start threads for the new migrators, now that we have the new_vb_table ready
        for nm in new_migrators:
            nm.start()
        Log.info("New table after parsing config: ")
        for (k, v) in new_vb_table.iteritems():
            Log.info(str(v))

        if (len(err_details)):
            return False
            #return json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":err_details})
        else:
            return True
            #return json.dumps({"Cmd":"Config", "Status":"OK"})

    def get(self, key):
        v = self.vbtable[key]
        return self.vbtable[key]

class Migrator(threading.Thread):
    
    def __init__(self, key, mm):
        super(Migrator, self).__init__()
        self.stop = threading.Event()
        self.key = key
        self.mm = mm
        self.master_items = {}
        self.replica_items = {}
        self.vbm_items = {}

    def is_tap_registered(self, source):
        (host, port) = source.split(':')
        # Get checkpoint stats from the membase. If the tap is registered, it will be listed in the stats
        cmd_str = "echo " + CHECKPOINT_STATS_STR + " | nc " + host + " " + port + "| grep repli-" + ("%X" % zlib.crc32(self.key))
        statp = subprocess.Popen([cmd_str], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (statout, staterr) = statp.communicate()
        if (statout != ''):
            return 1

        return 0            

    def start_vbm(self):
        try:
            row = self.mm.get(self.key)
            source = row.get('source')
            dest = row.get('destination')
            vblist = row.get('vblist')
            interface = row.get('interface')
            tapname = "repli-" + ("%X" % zlib.crc32(self.key))

            # Launch the VBM with the appropriate parameters
            # Register the replication tap
            if (self.is_tap_registered(source) == 0):
                regp = subprocess.Popen(["python", TAP_REGISTRATION_SCRIPT_PATH, "-h", source, "-r", tapname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (regout, regerr) = regp.communicate()
                if (regout != '' or regerr != ''):  # TODO go through the reg script and see what the error strings can be
                    Log.warning('Error registering the tap, stdout: [%s], stderr: [%s]',regout, regerr)
                    errmsg = "Error registering the tap, stdout: [" + regout + "], stderr: [" + regerr + "]"
                    err = create_error(source, dest, vblist, errmsg)
                    errqueue.put(err)
                    return 1

            # Start the VBM
            vblist_str = ",".join(str(vb) for vb in vblist)
            if (interface == ''):
                self.vbmp = subprocess.Popen(["sudo", VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", "-v", "-r"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
            else:
                self.vbmp = subprocess.Popen(["sudo", VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", "-i", interface, "-v", "-r"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
            Log.debug('Started VBM with pid %d interface %s', self.vbmp.pid, interface)
            # Wait for a couple of seconds to see if all is well # TODO Something better?
            start = time.time()
            while (time.time() - start < 2): 
                if (self.vbmp.poll() != None):      # means the VBM has exited
                    (vbmout, vbmerr) = self.vbmp.communicate()
                    Log.warning('Error starting the VBM between %s and %s for vbuckets %s on interface %s. Error: [%s]', source, dest, vblist_str, interface, vbmerr)
                    errmsg = "Error starting the VBM between " + source + " and " + dest + " for vbuckets [" + vblist_str + "] on interface " + interface + ". Error = [" + str(vbmerr) + "]"
                    err = create_error(source, dest, vblist, errmsg)
                    errqueue.put(err)
                    return 1
                else:
                    time.sleep(0.5)

        except KeyError:
            Log.warning('Unable to find key %s in vb table while starting VBM', self.key)
            errmsg = "Unable to find key " + self.key + " in mm vbtable"
            err = create_error(source, dest, vblist, errmsg)
            errqueue.put(err)
            return 1

        return 0

    def setup_vbuckets(self):
        try:
            row = self.mm.get(self.key)
            source = row.get('source')
            dest = row.get('destination')
            vblist = row.get('vblist')
            errmsg = ''

            for vb in vblist:
                # Mark vbucket as active on master
                master_vbucketctlp = subprocess.Popen([MBVBUCKETCTL_PATH, source, "set", str(vb), "active"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (vbout, vberr) = master_vbucketctlp.communicate()
                if (vberr != ''):
                    Log.warning('Error marking vbucket %d as active on %s. error; [%s]', vb, source, vberr)
                    errmsg = errmsg + " Error marking vbucket " + str(vb) + " as active on " + source
             
                # Mark vbucket as replica on slave            
                if (dest != ''):
                    slave_vbucketctlp = subprocess.Popen([MBVBUCKETCTL_PATH, dest, "set", str(vb), "replica"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    (vbout, vberr) = slave_vbucketctlp.communicate()
                    if (vberr != ''):
                        Log.warning('Error marking vbucket %d as replica on %s. error: [%s]', vb, dest, vberr)
                        errmsg = errmsg + " Error marking vbucket " + str(vb) + " as replica on " + dest

        except KeyError:
            Log.warning('Unable to find key %s in vb table while setting up vbuckets', self.key)
            errmsg = "Unable to find key " + self.key + " in mm vbtable"

        if (errmsg != ''):
            err = create_error(source, dest, vblist, errmsg)
            errqueue.put(err)
            return 1

        return 0

    def set_vbucket_state(self, host, vblist, state):
        errmsg = ''

        for vb in vblist:
            vbucketctlp = subprocess.Popen([MBVBUCKETCTL_PATH, host, "set", str(vb), state], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (vbout, vberr) = vbucketctlp.communicate()
            if (vberr != ''):
                Log.warning('Error marking vbucket %d as %s on %s. error; [%s]', vb, state, host, vberr)
                errmsg = errmsg + " Error marking vbucket " + str(vb) + " as " + state + " on " + host
         
        if (errmsg != ''):
            err = create_error(host, '', vblist, errmsg)
            errqueue.put(err)
            return 1

        return 0



    def get_vb_items(self, addr):
        vb_items = {}
        (host, port) = addr.split(':')
        vb_cmd = "echo " + VBUCKET_STATS_STR + " | nc " + host + " " +  port
        vbp = subprocess.Popen([vb_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (vbout, vberr) = vbp.communicate()
        if (vbout == '' or vberr != ''):
            Log.warning('Error getting vbucket count for host %s, error: [%s]', addr, vberr)
        for line in vbout.splitlines():
            m = re.match("STAT vb_(\d+) \w+ curr_items (\d+) kvstore .*", line)
            if (m != None):
                vb_items[int(m.group(1))] = int(m.group(2))

        return vb_items

    def check_vbm_progress(self):
        row = self.mm.get(self.key)
        dest = row.get('destination')
        vblist = row.get('vblist')
        (host, port) = dest.split(':')
        vbm_stats_sock = VBM_STATS_SOCK + "." + host
        vbm_stats_cmd = "echo stats | sudo nc -U " + vbm_stats_sock
        vbmp = subprocess.Popen([vbm_stats_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (vbmout, vbmerr) = vbmp.communicate()
        if (vbmout == '' or vbmerr != ''):
            Log.warning('Error getting stats from the vbucket migrator on socket %s. Error: [%s]', vbm_stats_sock, vbmerr)
            return False

        new_vbm_items = {}
        for line in vbmout.splitlines():
            m = re.match(r'vb:(\d+) rcvd:(\d+) sent:(\d+)', line)
            if (m != None):
                vb = int(m.group(1))
                rcvd = int(m.group(2))
                sent = int(m.group(3))
                new_vbm_items[vb] = (rcvd, sent)
            else:
                Log.warning('vbucket migrator stats output [%s] not in expected format', line)
                return False

        if (len(self.vbm_items) == 0):
            self.vbm_items = new_vbm_items
            return True

        retval = False
        for vb in vblist:
            try:
                (old_rcvd_count, old_sent_count) = self.vbm_items.get(vb)
                (new_rcvd_count, new_sent_count) = new_vbm_items.get(vb)
            except KeyError:
                retval = False
                break

            if (old_rcvd_count != new_rcvd_count or old_sent_count != new_sent_count):  # Some progress
                retval = True
                break
            else:
                Log.debug('No progress for vbucket %d', vb)

        self.vbm_items = new_vbm_items
        return retval


    def check_replication_status(self):
        row = self.mm.get(self.key)
        source = row.get('source')
        dest = row.get('destination')
        vblist = row.get('vblist')
        # Get count of items per vbucket on master
        new_master_items = self.get_vb_items(source)
        # Get count of items per vbucket on replica
        new_replica_items = self.get_vb_items(dest)
    
        if (len(new_master_items) == 0 or len(new_replica_items) == 0):
            return True

        if (len(self.master_items) == 0):
            self.master_items = new_master_items
            self.replica_items = new_replica_items
            return True

        vbm_progress = self.check_vbm_progress()
        retval = True
        for vb in vblist:
            try:
                old_master_count = self.master_items.get(vb)
                new_master_count = new_master_items.get(vb)
                old_replica_count = self.replica_items.get(vb)
                new_replica_count = new_replica_items.get(vb)
            except KeyError:
                Log.warning('Error finding item count for vbucket %d', vb)
                retval = False
                break
            
            master_activity = False
            if (new_master_count != old_master_count):
                master_activity = True

            if (master_activity == True):
                if ((new_master_count - new_replica_count) > DEFAULT_REPLICATION_DIFF):
                    # Check if the VBM is stuck 
                    if (vbm_progress == False):
                        Log.warning('Vbucketmigrator seems to be stuck, will restart it')
                        retval = False
                        break
                    else:
                        Log.warning('Replication lagging behind for vbucket %d on destination %s. Count on master %d, count on replica %d', vb, dest, new_master_count, new_replica_count)

        self.master_items = new_master_items
        self.replica_items = new_replica_items
        return retval
        

    def run(self):
        try:
            row = self.mm.get(self.key)
            source = row.get('source')
            dest = row.get('destination')
            vblist = row.get('vblist')

            # Mark vbucket as active on master
            if (self.set_vbucket_state(source, vblist, "active") != 0):
                return

            if (dest == ''):
                Log.info('Empty destination for key %s, vbucket list %s, so no replication to be done', self.key, row.get('vblist'))
                return 

            # Mark vbucket as replica on slave            
            if (self.set_vbucket_state(dest, vblist, "replica") != 0):
                self.set_vbucket_state(source, vblist, "dead")
                return

            if (self.start_vbm() != 0):
                self.set_vbucket_state(source, vblist, "dead")
                self.set_vbucket_state(dest, vblist, "dead")
                return

            cnt = 0;
            # Run for as long as we havent been asked to stop
            restart_vbm = False
            while not self.stop.isSet():
                if (self.vbmp.poll() != None or restart_vbm == True):
                    if (restart_vbm == True):
                        os.kill(self.vbmp.pid, signal.SIGTERM)
                        time.sleep(1)
                        restart_vbm = False

                    # Mark vbucket as replica on slave            
                    # (Doing this again because VBM might have died because the remote MB died.)
                    if (self.set_vbucket_state(dest, vblist, "replica") != 0):
                        time.sleep(10)                      # Some time before you retry
                        continue;

                    if (self.start_vbm() != 0):
                        time.sleep(10)                      # Some time before you retry
                        continue;


                time.sleep(1)       # TODO Something better?
                cnt = cnt + 1
                if (cnt == 10):
                    cnt = 0
                    if (self.check_replication_status() == False):
                        restart_vbm = True
        except Exception, e:
            Log.warning('Exiting thread %s because of exception: [%s]', self.key, str(e))

        # Marking all vbuckets as dead here. The next thread to take it up will mark these as active. 
        # (Doing this here to handle vbuckets that get "moved"
        self.set_vbucket_state(source, vblist, "dead")
        self.set_vbucket_state(dest, vblist, "dead")
        # Stop - kill the VBM and return
        Log.info('Stop request for key %s, will kill the vbucket migrator (pid %d)', self.key, self.vbmp.pid)
        os.kill(self.vbmp.pid, signal.SIGTERM)

    def join(self, timeout=None):
        self.stop.set()
        super(Migrator, self).join(timeout)

class SocketWrapper:

    def __init__(self, h, p):
        self.host = h
        self.port = p

    def recv_data(self, req_data_len):
        data = ''
        while (len(data) < req_data_len):
            try:
                chunk = self.s.recv(req_data_len - len(data))        
            except socket.timeout:
                return ''
            if (chunk == ''):
                self.s.close()
                raise RuntimeError("socket connection broken")
            data += chunk

        return data


    def send_data(self, data, len):
        sent_len = 0
        self.s.send(struct.pack('>I', len))
        while(sent_len < len):
            try:
                l1 = self.s.send(data[sent_len:])
                if (l1 == 0):
                    raise RuntimeError("socket connection broken")
            except socket.timeout:
                return sent_len
            sent_len += l1        
        return sent_len

    def connect(self):
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            self.s.settimeout(DEFAULT_SLEEP)
            if (self.host != DEFAULT_VBS_HOST):
                self.s.setsockopt(socket.SOL_SOCKET, IN.SO_BINDTODEVICE, "eth0"+'\0')
            self.s.connect((self.host, self.port))
            return ''
        except socket.timeout:
            return 'Socket timed out in connect'
        except socket.error, (value,message):
            if self.s: 
                self.s.close() 
            return message                

    def settimeout(self, t):
        self.s.settimeout(t)

def read_command(sock):
    """Receive from the socket and parse to find command name"""

    cmd_len = -1
    data = sock.recv_data(MIN_DATA_LEN)
    if (len(data) < MIN_DATA_LEN):
        return ''

    # The first 4 bytes are the length
    cmd_len, = struct.unpack('>I', data)     
    
    cmd = sock.recv_data(cmd_len)
    if (len(cmd) < cmd_len):
        return ''

    return cmd    

def create_error(source, dest, vblist, errmsg):
    return {"source":source, "destination":dest, "vblist":vblist, "error":errmsg}


def get_ifaces():
    # Get ALL interfaces name to IP addr mapping
    up_ifaces = {}
    ifp = subprocess.Popen(["/sbin/ifconfig"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (ifout, iferr) = ifp.communicate()
    for line in ifout.split("\n\n"):
        if (line != ''):
            (iname, iinfo) = line.split(None, 1)
            if (iname != 'lo' and re.search(r'\s+UP .* RUNNING .*$', iinfo, flags=re.MULTILINE)):
                m = re.search(r'inet addr:(.*)\s+Bcast', iinfo, flags=re.MULTILINE)
                if (m != None):
                    up_ifaces[iname] = m.group(1).strip()

    Log.debug('Interface info: %s', str(up_ifaces)) 
    return up_ifaces

def get_iface_for_ip(ip, ifaces):
    for (iface1, ip1) in ifaces.iteritems():
        if ip1 == ip:
            return iface1
    return ''


# Not used as of now
def get_mb_vblist(port):
    active_list = []
    replica_list = []
    host = "127.0.0.1"
    # Get checkpoint stats from the membase. If the tap is registered, it will be listed in the stats
    vbuckets_cmd = "echo " + VBUCKET_STATS_STR + " | nc " + host + " " + str(port)
    vbp = subprocess.Popen([vbuckets_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (statout, staterr) = vbp.communicate()
    if (statout == ''):
        return (active_list, replica_list)

    for line in statout.split("\n"):
        rmatch = re.match(r'STAT vb_([0-9]+) replica', line)
        if (rmatch):
            replica_list.append(rmatch.group(1))    
        amatch = re.match(r'STAT vb_([0-9]+) active', line)
        if (amatch):
            active_list.append(amatch.group(1))
    
    return (active_list, replica_list)

                 
def handle_init_cmd(cmd):                 
    # Read disk configuration
    global mb_host, mb_port
    disk_count_cmd = "echo " + KVSTORE_STATS_STR + " | nc " + mb_host + " " + str(mb_port) + " | grep \"status online\" | wc -l"
    dlp = subprocess.Popen([disk_count_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (dlout, dlerr) = dlp.communicate()
    if (dlout == '' or dlerr != ''):
        Log.warning('No kvstores are online. Check if Membase is up')
        disk_count = 0
    else:
        # Read the bad disk file
        disk_count = int(dlout)

    return json.dumps({'Agent':'VBA', 'Capacity':disk_count})


def check_mb_status():
    # Check if memcached is listening on some TCP port
    mb_check_cmd = "sudo netstat -plnt | grep memcached"
    mcp = subprocess.Popen([mb_check_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (mcout, mcerr) = mcp.communicate()
    if (mcout == '' or mcerr != ''):
        return 0

    for line in mcout.splitlines():       
        (p,sq,rq,la,rest) = line.split(None,4)
        (rest,port) = la.rsplit(':',1)
        if (port != ''):
            return int(port)

    return 0

def get_mb_pid():
    pid_file = open(MB_PID_FILE)
    pid = pid_file.readline()
    pid_file.close()
    return pid

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


if __name__ == '__main__':

    mypid = os.getpid() 
    pidfile = open(VBA_PID_FILE, 'w')
    pidfile.write(str(mypid))
    pidfile.close()

    #Log.basicConfig(filename='/var/log/vba.log', format='%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=Log.DEBUG)
    errqueue = Queue.Queue()
    heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL
    last_heartbeat = 0
    ifaces = get_ifaces()
    # Read VBS address
    vbs_host = DEFAULT_VBS_HOST
    vbs_port = DEFAULT_VBS_PORT
    opts, args = getopt.getopt(sys.argv[1:], 'f:')
    if (len(opts) != 0):
        parse_options(opts)
    vbs_port = int(vbs_port)
    Log.info('Vbucket Agent starting, Vbucket Server at %s:%d ', vbs_host, vbs_port)
    as_mgr = AsynCon()

    # get membase info - host, port and PID
    mb_host = DEFAULT_MB_HOST
    mb_port = check_mb_status()
    if (mb_port == 0):
        Log.warning('Membase not up on the machine, will exit')
        sys.exit()
    mb_pid = get_mb_pid()
    Log.info('Membase at %s:%d, membase PID %s', mb_host, mb_port, mb_pid)

    mm = MigrationManager(vbs_host, vbs_port)
    mb_mgr = MembaseManager(as_mgr) #TODO
    vbsManager = VBSManager(vbs_host, vbs_port, as_mgr, mm, mb_mgr, errqueue)
    Log.info("Starting async loop")
    as_mgr.loop()
    Log.info("Exitting")

    """vbs_sock = SocketWrapper(vbs_host, vbs_port)
    connected = False
    # listen for commands
    while 1:
        # connect to the VBS
        if (connected == False):
            errormsg = vbs_sock.connect()
            if (errormsg != ''):
                Log.warning('Could not open socket to VBS at %s:%d: [%s]', vbs_host, vbs_port, errormsg)
                time.sleep(5)
                continue                            
                    
        connected = True
        try:
            json_cmd = read_command(vbs_sock)
            if (json_cmd != ''):
                decoded_cmd = json.loads(json_cmd)
                cmd_name = decoded_cmd.get('Cmd')

                if (cmd_name == INIT_CMD_STR):
                    init_resp_str = handle_init_cmd(decoded_cmd)
                    vbs_sock.send_data(init_resp_str, len(init_resp_str))
                elif (cmd_name == CONFIG_CMD_STR):
                    config_resp_str = mm.handle_new_config(decoded_cmd, ifaces)
                    vbs_sock.settimeout(heartbeat_interval)
                    vbs_sock.send_data(config_resp_str, len(config_resp_str))
                else:
                    #error
                    Log.warning('Unknown command from VBS: %s', cmd_name)
                    unknown_cmd_str = json.dumps(UNKNOWN_CMD_JSON)
                    vbs_sock.send_data(unknown_cmd_str, len(unknown_cmd_str))

            #send heartbeat or error messages (if any)
            now = time.time()
            if (errqueue.empty() == False):
                err = errqueue.get_nowait()
                err_json = json.dumps(err)
                vbs_sock.send_data(err_json, len(err_json))
                last_heartbeat = now
            elif (now - heartbeat_interval >= last_heartbeat):
                vbs_sock.send_data(HEARTBEAT_CMD, len(HEARTBEAT_CMD))
                last_heartbeat = now

            new_mb_pid = get_mb_pid()
            if (new_mb_pid != mb_pid):
                Log.info('Membase seems to have restarted, exiting')
                sys.exit()

        except socket.error, (value,message):
            Log.warning('Got socket error [%s]', message)
            connected = False
        except RuntimeError, (message):
            Log.warning('Got socket runtime error [%s]', str(message))
            connected = False
    """

