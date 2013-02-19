import string
import socket
import exceptions
import struct
import errno
import time

from logger import *
from asyncon import *

Log = getLogger()

class MembaseHandler(AsynConDispatcher):
    VERSION_MONITORING = 1
    STATS_MONITORING = 2
    STATS_COMMAND = "START_STATS"
    START_COMMAND = "START"
    MEMBASE_PORT = 11211
    ERROR_THRESHOLD = 5
    HEALTHY = 1
    FAIL = 2
    RECONNECT = 3
    STATS_TERM = "END"
    ITEM_COUNT_KEY = "curr_items"
    MEMBASE_VERSION_CMD = "version\r\n"
    MEMBASE_STATS_CMD = "stats\r\n"
    TOT_RETRY_COUNT = 3
    CUR_ITEMS_DELTA = 500

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
        self.half_baked = True
        self.send_count = 0
        self.recv_count = 0
        self.timeout = 10
        port = None
        self.map = None
        self.mgr = None
        self.aok_ts = time.time()
        self.read_callback = None
        self.monit_ip = None

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
        if(params.has_key('map')):
            self.map = params['map']
        if(params.has_key('mgr')):
            self.mgr = params['mgr']
        if(params.has_key('type')):
            if params['type'] == MembaseHandler.STATS_COMMAND :
                self.command_type = MembaseHandler.STATS_MONITORING
            else:
                self.command_type = MembaseHandler.VERSION_MONITORING
                self.half_baked = False

        AsynConDispatcher.__init__(self, None, self.timeout, self.mgr)
        #Creates non blocking socket
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.addr is None:
            self.connect((self.ip, port))
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
        while True:
            try:
                self.rbuf += self.recv(self.buffer_size)
                Log.debug(self.rbuf)
            except socket.error, why:
                if why[0] == errno.EAGAIN:
                    break
                else:
                    Log.error("Read event error %s" % why)
                    self.rbuf = ""
                    self.handle_error()
                    return
        if self.command_type == MembaseHandler.VERSION_MONITORING:   
            self.handle_version_read()
        else:
            self.handle_stats_read()
        if not self.read_callback is None:
            self.read_callback(self)

    def handle_stats_read(self):
        if self.rbuf.find(MembaseHandler.STATS_TERM) > 0:
            stats_map = {}
            msg = self.rbuf[:len(self.rbuf)]
            self.rbuf = ""
            data_arr = msg.split("\r\n")
            for row in data_arr:
                data = row.split(' ')
                if len(data) > 2:
                    stats_map[data[1]] = data[2]
            if stats_map.has_key(MembaseHandler.ITEM_COUNT_KEY):
                self.remote_item_count = int(stats_map[MembaseHandler.ITEM_COUNT_KEY])
                self.recv_count += 1
                if self.local_item_count != -1 and (self.remote_item_count - self.local_item_count) < MembaseHandler.CUR_ITEMS_DELTA:
                    self.half_baked = False

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
        self.write_data(MembaseHandler.MEMBASE_STATS_CMD)

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
            if self.command_type == MembaseHandler.STATS_MONITORING:
                Log.debug("Item counts: Local: %d Remote %d" %(self.local_item_count, self.remote_item_count))
                if (self.remote_item_count >= 0) and (self.local_item_count > 0)  and ((self.remote_item_count - self.local_item_count) < MembaseHandler.CUR_ITEMS_DELTA):
                    self.half_baked = False
                    Log.info("Slave is fully functional")
                if not self.half_baked:
                    #Switch to command type VERSION
                    Log.info("Switching to command type VERSION")
                    self.command_type = MembaseHandler.VERSION_MONITORING
                    self.send_version()
                else:
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
