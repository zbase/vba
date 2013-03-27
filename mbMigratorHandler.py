import string
import socket
import exceptions
import struct
import errno
import time

from logger import *
from asyncon import *

Log = getLogger()

class MBMigratorHandler(AsynConDispatcher):
    MAX_TIMEOUT = 100
    STATS_TERM = "END"
    
    def __init__(self, params):
        self.saddr = None
        self.timer = None
        self.stats = None
        self.mgr = None
        self.name= ""
        self.last_response_ts = int(time.time())
        self.read_callback = None
        self.rbuf = ""

        if params.has_key('addr'):
            self.saddr = params['addr']
        if params.has_key('name'):
            self.name = params['name']
        if params.has_key('timeout'):
            self.timer = params['timeout']
        if params.has_key('mgr'):
            self.mgr = params['mgr']
        if params.has_key('readCallback'):
            self.read_callback = params['readCallback']

        (socket.AF_UNIX, socket.SOCK_STREAM)

        AsynConDispatcher.__init__(self, None, self.timer, self.mgr)
        self.create_timer()
        self.set_timer()
    
    def set_timer(self):
        self.timer_event.add(self.timer)

    def handle_timer(self):
        #Do the unix domain socket work here
        Log.debug("Timeout... will make the uds call")
        cur_time = int(time.time())
        if cur_time - self.last_response_ts > MBMigratorHandler.MAX_TIMEOUT:
            Log.critical("VBucket Migrator %s is not responding")
            self.handle_fail()
            return

        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.connect(self.saddr)
            self.create_events()
            self.set_timer()
            self.enable_read()
        except Exception, e:
            Log.error("Unable to connect to the VBucketMigrator")
            self.handle_fail()
        
    def handle_read(self):
        #Read response from vBucketMigrator
        #Parse stats and set in stats map
        #Update the timestamp
        response = None
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
                    self.handle_fail()
                    return
        self.stats= self.handle_stats_read()
        
        if not response is None :
            self.last_response_ts = int(time.time())
            if not self.read_callback is None:
                self.read_callback(self, response)

    def handle_stats_read(self):
        if self.rbuf.find(MBMigratorHandler.STATS_TERM) > 0:
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
            return stats_map
        return None

    def handle_fail(self):
        Log.critical("VBucketMigrator %s is not responding. Will try restarting")
        self.destroy()

    def destroy(self):
        self.close()
