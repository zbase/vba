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

INIT_CMD_STR = "INIT"
CONFIG_CMD_STR = "CONFIG"
HEARTBEAT_CMD_STR = "Alive"
DEFAULT_HEARTBEAT_INTERVAL = 120
DEFAULT_SLEEP = 10

INVALID_CMD     = 0
INIT_CMD        = 1
CONFIG_CMD      = 2

MIN_DATA_LEN    = 4 # The first 4 bytes of a packet give us the length 

TAP_REGISTRATION_SCRIPT_PATH = "/home/vnatarajan/workspace/multivbucket/ep-engine/management/mbadm-tap-registration"
VBUCKET_MIGRATOR_PATH = "/home/vnatarajan/workspace/multivbucket/vbucketmigrator/vbucketmigrator"
MBVBUCKETCTL_PATH = "/home/vnatarajan/workspace/multivbucket/ep-engine/management/mbvbucketctl"

CONFIG_CMD_OK_JSON = '{"Cmd":"Config", "Status":"OK"]}'
UNKNOWN_CMD_JSON = '{"Cmd":"Unknown", "Status":"ERROR"}'

class MigrationManager:

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

    def handle_iface_change(self, ifaces):
        i = 0
        if (len(ifaces) < 1):
            print "Need at least one interface"
            # TODO Some error?
            return

        iface_count = len(ifaces)
        for (k, v) in self.vbtable.iteritems():
            print "Curr iface " + v['interface'] + " final iface " + ifaces[i%iface_count] 
            if (v['interface'] != ifaces[i%iface_count]):
                v['interface'] = ifaces[i%iface_count]  
                self.end_migrator(k)
                nm = self.create_migrator(k,v)
                nm.start()
            i = i+1

        print "Final table"
        print self.vbtable

    def handle_new_config(self, config_cmd, ifaces):
        new_vb_table = {}
        if ('HeartBeatTime' in config_cmd):
            heartbeat_interval = config_cmd['HeartBeatTime']
            print "Heartbeat = " + str(heartbeat_interval)

        config_data = config_cmd.get('Data')
        #config_data = config_cmd['Data']
        if (config_data == None or len(config_data) == 0):
            print "Vbucket map missing in config"   # TODO print where? Maybe call report_errors
            return json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":["No Vbucket map in config"]})

        print  config_data
        # Create the table from the config data
        # The config data is of the form
        # [
        #   {"source":"localhost:11211", "vblist":[1,2,3], "destination":"192.168.1.2:11211"},  
        #   {"source":"localhost:11611", "vblist":[7,8,9], "destination":"192.168.1.5:11211"},  
        #   .
        #   .
        # ]

        i = 0
        err_details = []
        iface_count = len(ifaces)
        for row in config_data:
            source = row.get('Source')
            if (source == ''):
                err_details += "Source missing"
            dest = row.get('Destination')
            if (dest == ''):
                err_details += " Destination missing"
            vblist = row.get('VbId')
            if (len(vblist) == ''):
                err_details += " Vbucket list missing"
            if (source == '' or dest == '' or len(vblist) == ''):
                err_details.append("For row [" + row + "], source/destination/vbucket list missing")
                continue
                
            vblist.sort()
            #vblist_str = ",".join(str(vb) for vb in vblist)
            key = source + "|" + dest
            value = {}
            value['source'] = source
            value['destination'] = dest
            value['vblist'] = vblist
            value['interface'] = ifaces[i%iface_count]
            print value
            new_vb_table[key] = value
            i=i+1

        new_migrators = []
        # Compare old and new vb tables
        if (len(self.vbtable) != 0):
            # Iterate over the new table and:
            #   If the row is present in the old table, restart the VBM
            #   else start up a new VBM
            for (k, v) in new_vb_table.iteritems():
                if k in self.vbtable:
                    if (self.vbtable[k]['vblist'] != v['vblist'] or
                        self.vbtable[k]['interface'] != v['interface']):
                        # Kill the existing VBM and start a new one
                        print "Replacing vblist " 
                        print self.vbtable[k]['vblist']
                        print "for key " + k + " with " 
                        print v['vblist']
                        print "Old interface " + self.vbtable[k]['interface'] + " with new interface " + v['interface']
                        self.end_migrator(k)
                        new_migrators.append(self.create_migrator(k,v))

                    else:
                        # just copy the migrator obj to the new table
                        print "Doing nothing for key " + k
                        v['migrator'] =  self.vbtable[k].get('migrator')
                else:
                    # Start a new VBM
                    print "Starting a new VBM for key " + k
                    new_migrators.append(self.create_migrator(k,v))

                # Iterate over the old table and:
                #   If the key is not found in the new table, kill the VBM for that row
                for (k, v) in self.vbtable.iteritems():
                    if k not in new_vb_table:
                        # Kill the VBM for the row
                        print "Killing the VBM for key " + k
                        self.end_migrator(k)
        else:   # First time, start VBMs for all rows in new_vb_table                            
            for (k, v) in new_vb_table.iteritems():
                new_migrators.append(self.create_migrator(k,v))


        self.vbtable = new_vb_table
        # Start threads for the new migrators, now that we have the new_vb_table ready
        for nm in new_migrators:
            nm.start()
        print "Final table"
        print self.vbtable

        if (len(err_details)):
            return json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":err_details})
        else:
            return json.dumps(CONFIG_CMD_OK_JSON)

    #def report_errors(self, error_str)

    def get(self, key):
        v = self.vbtable[key]
        print "Key " + key + " interface " + v['interface']
        return self.vbtable[key]

class Migrator(threading.Thread):
    
    def __init__(self, key, mm):
        super(Migrator, self).__init__()
        self.stop = threading.Event()
        #self.stop = 0
        self.key = key
        self.mm = mm


    def is_tap_registered(self, source):
        (host, port) = source.split(':')
        # Get checkpoint stats from the membase. If the tap is registered, it will be listed in the stats
        cmd_str = "echo stats checkpoint | nc " + host + " " + port + "| grep repli-" + ("%X" % zlib.crc32(self.key))
        print cmd_str
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
                    print "Error registering the tap, stdout: [" + regout + "], stderr: [" + regerr + "]"
                    errmsg = "Error registering the tap, stdout: [" + regout + "], stderr: [" + regerr + "]"
                    err = create_error(source, dest, vblist, errmsg)
                    errqueue.put(err)
                    return 1

            # Start the VBM
            vblist_str = ",".join(str(vb) for vb in vblist)
            self.vbmp = subprocess.Popen(["sudo", VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", "-i", interface, "-v", "–r"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
            print " Started VBM with pid " + str(self.vbmp.pid) + " interface " + interface
            # Wait for a couple of seconds to see if all is well # TODO Something better?
            start = time.time()
            while (time.time() - start < 2): 
                if (self.vbmp.poll() != None):      # means the VBM has exited
                    (vbmout, vbmerr) = self.vbmp.communicate()
                    print "Error starting the VBM between " + source + " and " + dest + " for vbuckets [" + vblist_str + "] on interface " + interface + ". Error = [" + str(vbmerr) + "]"
                    errmsg = "Error starting the VBM between " + source + " and " + dest + " for vbuckets [" + vblist_str + "] on interface " + interface + ". Error = [" + str(vbmerr) + "]"
                    err = create_error(source, dest, vblist, errmsg)
                    errqueue.put(err)
                    return 1
                else:
                    time.sleep(0.5)

        except KeyError:
            print "Unable to find key " + self.key + " in mm vbtable"    # TODO Report an error?
            errmsg = "Unable to find key " + self.key + " in mm vbtable"    # TODO Report an error?
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

            # TODO check if doing this again and again makes any diff 

            for vb in vblist:
                # Mark vbucket as active on master
                print "Marking vbucket " + str(vb) + " as active on " + source
                master_vbucketctlp = subprocess.Popen([MBVBUCKETCTL_PATH, source, "set", str(vb), "active"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (vbout, vberr) = master_vbucketctlp.communicate()
                if (vberr != ''):
                    print "Error marking vbucket " + str(vb) + " as active on " + source
                    errmsg = errmsg + " Error marking vbucket " + str(vb) + " as active on " + source
             
                # Mark vbucket as replica on slave            
                print "Marking vbucket " + str(vb) + " as replica on " + dest
                slave_vbucketctlp = subprocess.Popen([MBVBUCKETCTL_PATH, dest, "set", str(vb), "replica"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (vbout, vberr) = slave_vbucketctlp.communicate()
                if (vberr != ''):
                    print "Error marking vbucket " + str(vb) + " as replica on " + dest
                    errmsg = errmsg + " Error marking vbucket " + str(vb) + " as replica on " + dest

        except KeyError:
            print "Unable to find key " + self.key + " in mm vbtable"
            errmsg = "Unable to find key " + self.key + " in mm vbtable"

        if (errmsg != ''):
            create_error(source, dest, vblist, errmsg)
            errqueue.put(err)
            return 1

        return 0

    def run(self):
        print "Start for key " + self.key
        if (self.setup_vbuckets() != 0):
            print "Error setting up vbuckets for " + self.key
            return
        if (self.start_vbm() != 0):
            print "Return"
            return

        print "Will get into the while"
        # Run for as long as we havent been asked to stop
        while not self.stop.isSet():
        #while (self.stop == 0):
            if (self.vbmp.poll() != None):   # TODO have a check on max retries here?
                self.start_vbm()
            time.sleep(1)       # TODO Something better?

        # Stop - kill the VBM and return
        print "Will kill PID " + str (self.vbmp.pid)
        os.kill(self.vbmp.pid, signal.SIGTERM)   

    def join(self, timeout=None):
        self.stop.set()
        #self.stop = 1
        #threading.Thread.join(self,timeout)
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
                #TODO Print some error msg
                return chunk
            data += chunk

        return data


    def send_data(self, data, len):
        sent_len = 0
		self.s.send(struct.pack('>I', len))
		print "Sending data  " + data
        while(sent_len < len):
            l1 = self.s.send(data[sent_len:])
            if (l1 == 0):
                return sent_len
            sent_len += l1        

        return sent_len

    def connect(self):
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            self.s.settimeout(DEFAULT_SLEEP)
            self.s.connect((host, port))
            return ''
        except socket.error, (value,message):
            if self.s: 
                self.s.close() 
            return message


def read_command(sock):
    """Receive from the socket and parse to find command name"""

    cmd_len = -1
    #print "Reading " + str(MIN_DATA_LEN) + " bytes" 
    data = sock.recv_data(MIN_DATA_LEN)
    if (len(data) < MIN_DATA_LEN):
        return ''

    print "Read " + str(len(data)) + " bytes data = " + data 

    # The first 4 bytes are the length
    cmd_len, = struct.unpack('>I', data)     
    
    print "Len " + str(cmd_len) + " bytes" 

    cmd = sock.recv_data(cmd_len)
    print "For command, read " + str(len(cmd)) + " bytes, expected " + str(cmd_len) + " bytes, command " + cmd 
    if (len(cmd) < cmd_len):
        return ''

    return cmd    

def create_error(source, dest, vblist, errmsg):
    return {"source":source, "destination":dest, "vblist":vblist, "error":errmsg}

#def get_interfaces():
#    ifaces = []
#    ifp = subprocess.Popen(["/sbin/ifconfig"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#    (ifout, iferr) = ifp.communicate()
#    for line in ifout.split("\n\n"):
#        if (line != ''):
#            (iname, iinfo) = line.split(None, 1)
#            print "Interface name " + iname
#            if (iname != 'lo'):
#                ifaces.append(iname)
#
#    return ifaces

#def update_iface_status(ifaces):
#    up_ifaces = []
#    ifp = subprocess.Popen(["/sbin/ifconfig"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#    (ifout, iferr) = ifp.communicate()
#    for line in ifout.split("\n\n"):
#        if (line != ''):
#            (iname, iinfo) = line.split(None, 1)
#            print "Interface name " + iname
#            if (iname != 'lo' and re.search(r'\sUP .* RUNNING .*$', iinfo, flags=re.MULTILINE)):
#                up_ifaces.append(iname)
#
#    change = False
#    for (interface, state) in ifaces.iteritems():
#        if interface in up_ifaces and state == 0:
#            change = True
#            ifaces[interface] = 1
#        elif interfaces not in up_ifaces and state == 1:
#            change = True
#            ifaces[interface] = 0
#
#    return change

def get_up_ifaces(vba_ifaces):
    # Get ALL up interfaces
    up_ifaces = []
    ifp = subprocess.Popen(["/sbin/ifconfig"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (ifout, iferr) = ifp.communicate()
    for line in ifout.split("\n\n"):
        if (line != ''):
            (iname, iinfo) = line.split(None, 1)
            #print "Interface name " + iname
            if (iname != 'lo' and re.search(r'\sUP .* RUNNING .*$', iinfo, flags=re.MULTILINE)):
                up_ifaces.append(iname)

    # Find out which of the interfaces configured for VBA are up
    vba_up_ifaces = []
    for interface in vba_ifaces:
        if interface in up_ifaces:
            vba_up_ifaces.append(interface)

    print "Up ifaces " + str(vba_up_ifaces) 
    return vba_up_ifaces



def get_mb_vblist(port):
    active_list = []
    replica_list = []
    host = "127.0.0.1"
    # Get checkpoint stats from the membase. If the tap is registered, it will be listed in the stats
    vbuckets_cmd = "echo stats vbucket | nc " + host + " " + str(port)
    print vbuckets_cmd
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
        
    # TODO Read disk configuration
    disk_count = 6
    (active_vblist, replica_vblist) = get_mb_vblist(11211)
    
    # Create the response    
    return json.dumps({'Agent':'VBA', 'Capacity':disk_count, 'Vbuckets':{'Master':active_vblist, 'Replica':replica_vblist}})

    
if __name__ == '__main__':

    errqueue = Queue.Queue()

    heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL
    last_heartbeat = 0
    # TODO Read input config 
    vba_ifaces = ["eth0", "eth1"]
    up_ifaces = get_up_ifaces(vba_ifaces)
    # TODO Read VBS address
    host = '127.0.0.1'
    port = 11200

    mm = MigrationManager(host, port)

    vbs_sock = SocketWrapper(host, port)
    connected = False
    # listen for commands
    while 1:
        # connect to the VBS
        if (connected == False):
            errormsg = vbs_sock.connect()
            if (errormsg != ''):
                print "Could not open socket: " + errormsg   # TODO Print where?
                time.sleep(5)     # TODO What to do here? Contact the ANF component and complain? Likely even that will not be able to get to VBS
                                # Right now, we exit and the VBA monitoring script will start us up again
                continue                            
                    
        connected = True
        try:
            json_cmd = read_command(vbs_sock)
            if (json_cmd != ''):
                decoded_cmd = json.loads(json_cmd)
                cmd_name = decoded_cmd.get('Cmd')

                print "Command name = " + cmd_name 
                if (cmd_name == INIT_CMD_STR):
                    init_resp_str = handle_init_cmd(decoded_cmd)
                    vbs_sock.send_data(init_resp_str, len(init_resp_str))
                elif (cmd_name == CONFIG_CMD_STR):
                    print "Config it is!"
                    config_resp_str = mm.handle_new_config(decoded_cmd, up_ifaces)
                    vbs_sock.send_data(config_resp_str, len(config_resp_str))
                else:
                    #error
                    print "Error: Unknown command " + cmd_name
                    unknown_cmd_str = json.dumps(UNKNOWN_CMD_JSON)
                    vbs_sock.send_data(unknown_resp_str, len(unknown_resp_str))

            #send heartbeat or error messages (if any)
            now = time.time()
            if (errqueue.empty() == False):
                err = errqueue.get_nowait()
                err_json = json.dumps(err)
                vbs_sock.send_data(err_json, len(err_json))
                last_heartbeat = now
                print "Error in queue!"
                print err
            elif (now - heartbeat_interval >= last_heartbeat):
                #vbs_sock.send_data(HEARTBEAT_CMD)
                last_heartbeat = now

            new_up_ifaces = get_up_ifaces(vba_ifaces)
            if (up_ifaces != new_up_ifaces):
                print "New up iface list " + str(new_up_ifaces)
                up_ifaces = new_up_ifaces
                mm.handle_iface_change(up_ifaces)

        except socket.error, (value,message):
            print "Got socket error [" + message + "]"
            connected = False


