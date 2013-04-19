import string
import json
import socket
import exceptions
import struct
import errno
import time
import re

from logger import *
from asyncon import *

Log = getLogger()

class MembaseHandler(AsynConDispatcher):
    MEMBASE_PORT = 11211
    ERROR_THRESHOLD = 5
    VERSION_MONITORING, VB_STATS_MONITORING, KV_STATS_MONITORING, CP_STATS_MONITORING, HEALTHY, FAIL, RECONNECT = range(7)
    STATS_TERM = "END"
    ITEM_COUNT_KEY = "curr_items"
    MEMBASE_VERSION_CMD = "version\r\n"
    MEMBASE_VB_STATS_CMD = "stats vbucket\r\n"
    MEMBASE_KV_STATS_CMD = "stats kvstore\r\n"
    MEMBASE_CP_STATS_CMD = "stats checkpoint\r\n"
    TOT_RETRY_COUNT = 3
    CUR_ITEMS_DELTA = 500
    LOCAL = "127.0.0.1:11211"

    def __init__(self, params):
        sock = None
        self.callback = None
        self.ip = None
        self.retry_count = MembaseHandler.TOT_RETRY_COUNT
        self.failCallback = None
        self.link = None
        self.addr = None
        self.port = None
        self.local_item_count = -1
        self.remote_item_count = -1
        self.cmd_type = None
        self.send_count = 0
        self.recv_count = 0
        self.timeout = 10
        port = None
        self.map = None
        self.mgr = None
        self.mb_mgr = None
        self.aok_ts = time.time()
        self.read_callback = None
        self.monit_ip = None
        self.vb_stats = None
        self.kv_stats = None
        self.cp_stats = None
        self.host = None
        self.local = False

        if(params.has_key('ip')):
            self.ip = params['ip']
        if(params.has_key('monitIP')):
            self.monit_ip = params['monitIP']
        if(params.has_key('retryCount')):
            self.retry_count = params['retryCount']
        if(params.has_key('failCallback')):
            self.failCallback = params['failCallback']
        if(params.has_key('readCallback')):
            self.read_callback = params['readCallback']
        if(params.has_key("retry")):
            self.retry_count = params["retry"]
        if(params.has_key('addr')):
            self.addr = params['addr']
            self.ip = self.addr[0]
            self.port = self.addr[1]
        if(params.has_key('timeout')):
            self.timeout = params['timeout']
        if(params.has_key('port')):
            port = params['port']
            self.port = str(port)
        else:
            port = MembaseHandler.MEMBASE_PORT
            self.port = str(MembaseHandler.MEMBASE_PORT)
        if params.has_key('mb_mgr'):
            self.mb_mgr = params['mb_mgr']
        if params.has_key('local'):
            self.local = params['local']

        self.host = self.ip+":"+self.port
        if(params.has_key('map')):
            self.map = params['map']
        if(params.has_key('mgr')):
            self.mgr = params['mgr']
        if(params.has_key('type')):
            self.command_type = params['type']
        else:
            self.command_type = MembaseHandler.VERSION_MONITORING

        AsynConDispatcher.__init__(self, None, self.timeout, self.mgr)
        #Creates non blocking socket
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.addr is None:
            self.connect((self.ip, int(port)))
        else:
            self.connect(self.addr)

        self.rbuf = ""
        self.wbuf = ""
        self.buffer_size = 4096 
        self.closeSock = None 
        self.gotSize = False
        self.totSize = 0
        self.ipStr = (None, str(self.ip))[self.ip != None]
        self.ipStr += ("",":"+str(port))[port != None]
        if not self.map is None:
            self.map[self.ip+":"+self.port] = self
        self.enable_timeout()

    def handle_connect(self):
        Log.debug("Connected %s:%s" %(self.ip, self.port))
        pass

    def isConnected(self):
        return self.connected

    def destroy(self):
        if(self.connected == True):
            self.connected = False
            self.close()

    def handle_error(self):
        self.reconnect()

    def handle_read(self):
        response = None
        while True:
            try:
                self.rbuf += self.recv(self.buffer_size)
            except socket.error, why:
                if why[0] == errno.EAGAIN:
                    break
                else:
                    Log.error("Read event error %s" % why)
                    self.rbuf = ""
                    self.handle_error()
                    return
        if self.command_type == MembaseHandler.VERSION_MONITORING:   
            response = self.handle_version_read()
        elif self.command_type == MembaseHandler.VB_STATS_MONITORING:
            response = self.handle_vb_stats_read()
        elif self.command_type == MembaseHandler.KV_STATS_MONITORING:
            response = self.handle_kv_stats_read()
        elif self.command_type == MembaseHandler.CP_STATS_MONITORING:
            response = self.handle_cp_stats_read()
        if ((not response is None) and (not self.read_callback is None)):
            self.read_callback(self, response)

    def handle_vb_stats_read(self):
        if self.rbuf.find(MembaseHandler.STATS_TERM) > 0:
            self.recv_count += 1
            stats_map = {}
            msg = self.rbuf[:len(self.rbuf)]
            self.rbuf = ""
            for row in msg.splitlines():
                data = row.split(' ')
                if len(data) == 7:
                    stat_map = {}
                    vb = int((data[1].split('_'))[1])
                    stat_map['state'] = data[2]
                    stat_map[data[3]] = data[4]
                    stat_map[data[5]] = data[6]
                    stats_map[vb] = stat_map
            self.vb_stats = stats_map
            return stats_map
        return None

    def handle_kv_stats_read(self):
        if self.rbuf.find(MembaseHandler.STATS_TERM) > 0:
            self.recv_count += 1
            stats_map = {}
            msg = self.rbuf[:len(self.rbuf)]
            self.rbuf = ""
            for row in msg.splitlines():
                data = row.split(' ')
                if len(data) == 3:
                    stats_map[data[1]] = data[2]
            self.kv_stats = stats_map
            return stats_map
        return None

    def handle_cp_stats_read(self):
        if self.rbuf.find(MembaseHandler.STATS_TERM) > 0:
            self.recv_count += 1
            stats_map = {}
            msg = self.rbuf[:len(self.rbuf)]
            self.rbuf = ""
            regex = re.compile('[: ]')
            for row in msg.splitlines():
                data = regex.split(row)
                if len(data) == 4:
                    vb_arr = data[1].split('_')
                    if len(vb_arr) != 2:
                        continue
                    vb = int(vb_arr[1])
                    v_map = {}
                    if stats_map.has_key(vb):
                        v_map = stats_map[vb]
                    v_map[data[2]] = data[3]
                    stats_map[vb] = v_map

            #Compare with existing map and report to vbs if there is a difference
            if not self.local:
                return stats_map

            to_send = False
            if self.cp_stats is None:
                to_send = True
                Log.debug("Sending checkpoint stats on init")
            else:
                for vb, cp_map in stats_map.items():
                    if (cp_map['state'] == 'active' or cp_map['state'] == 'replica') :
                        if cp_map['open_checkpoint_id'] != self.cp_stats[vb]['open_checkpoint_id']:
                                Log.debug("Checkpoints not matching for vb: %s %s %s" %(vb, cp_map['open_checkpoint_id'], self.cp_stats[vb]['open_checkpoint_id']))
                                to_send = True
                                break
                        else:
                            Log.debug("Error for %s while checking vBucket %d map: %s" %(self.ip, vb, cp_map))
                            to_send = True
                            break
            
            self.cp_stats = stats_map
            if to_send:
                self.send_checkpoints()

            return stats_map
        return None

    def send_checkpoints(self):
        active_vb = []
        replica_vb = []
        active_cp = []
        replica_cp = []

        for vb, cp_map in self.cp_stats.items():
            if cp_map['state'] == 'active':
                active_vb.append(vb)
                active_cp.append(int(cp_map['open_checkpoint_id']))
            else:
                replica_vb.append(vb)
                replica_cp.append(int(cp_map['open_checkpoint_id']))

        msg = {'Cmd':'CKPOINT', 'Vbuckets':{'Active':active_vb, 'Replica':replica_vb}, 'CheckPoints':{'Active':active_cp, 'Replica':replica_cp}}
        self.mb_mgr.report_stats(json.dumps(msg))

    def handle_version_read(self):
        if self.rbuf.find("VERSION") >= 0:
            self.recv_count += 1
            self.rbuf = ""

    def write_data(self, msg):
        self.wbuf += msg
        self.enable_write()

    def send_version(self):
        self.write_data(MembaseHandler.MEMBASE_VERSION_CMD)

    def send_stats(self):
        if self.command_type == MembaseHandler.VB_STATS_MONITORING:
            self.write_data(MembaseHandler.MEMBASE_VB_STATS_CMD)
        elif self.command_type == MembaseHandler.CP_STATS_MONITORING:
            self.write_data(MembaseHandler.MEMBASE_CP_STATS_CMD)
        else:
            self.write_data(MembaseHandler.MEMBASE_KV_STATS_CMD)

    def set_read(self):
        status = True
        try:
            self.enable_read()
        except:
            Log.error("Error setting read event!")
            status = False
            self.handle_error()
        return status

    def handle_write(self):
        try:
            if len(self.wbuf) > 0:
                sent = self.send(self.wbuf)
                self.wbuf = self.wbuf[sent:]
                self.send_count += 1
                self.set_read()
            else:
                Log.debug("Write on empty buffer!")
        except socket.error, why:
            Log.error("Exception when trying to write %s. Reconnecting and retrying %s" %(why, self.wbuf))
            self.reconnect()
            self.enable_write()

    def reconnect(self):
        self.send_count = 0
        self.recv_count = 0
        self.retry_count -= 1
        if self.retry_count < 0:
            self.failCallback(self)
            return
        self.destroy()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((self.ip, int(self.port)))
        self.enable_timeout()

    def health(self):
        if self.connected and (self.retry_count > 0) and ((self.send_count - self.recv_count) < MembaseHandler.ERROR_THRESHOLD):
            Log.debug("Counts: s:%d r:%d" %(self.send_count, self.recv_count))
            return MembaseHandler.HEALTHY
        elif self.retry_count <= 0:
            return MembaseHandler.FAIL
        else:
            return MembaseHandler.RECONNECT

    def handle_timeout(self):
        self.aok_ts = time.time()
        con_health = self.health()
        if con_health == MembaseHandler.HEALTHY:
            # Check of connection is healthy
            # Check sent and received count
            if self.command_type == MembaseHandler.VB_STATS_MONITORING:
                self.command_type = MembaseHandler.KV_STATS_MONITORING
                self.send_stats()
            elif self.command_type == MembaseHandler.CP_STATS_MONITORING:
                self.command_type = MembaseHandler.VB_STATS_MONITORING
                self.send_stats()
            elif self.command_type == MembaseHandler.KV_STATS_MONITORING:
                self.command_type = MembaseHandler.CP_STATS_MONITORING
                self.send_stats()
            else:
                self.send_version()
            self.enable_timeout()
        elif con_health == MembaseHandler.RECONNECT:
            self.reconnect()
        else:
            self.failCallback(self)
            self.close()
                
    def handle_close(self):
        self.close()

    def handler_error(self):
        self.destroy()
