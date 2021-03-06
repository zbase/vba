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
import string
import socket
import exceptions
import struct
import errno
import time
import re

from vbaLogger import *
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
        self.buffer_size = 4096
        self.connected = False
        self.wbuf = ""

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

        AsynConDispatcher.__init__(self, None, self.timer, self.mgr)
        self.create_timer()
        self.set_timer()

    def handle_connect(self):
        Log.error("Connected")
        self.connected = True

    def handle_close(self):
        self.destroy()
    
    def set_timer(self):
        self.timer_event.add(self.timer)

    def handle_timer(self):
        #Do the unix domain socket work here
        Log.error("Timeout... will make the uds call")
        cur_time = int(time.time())
        if cur_time - self.last_response_ts > MBMigratorHandler.MAX_TIMEOUT:
            Log.critical("VBucket Migrator %s is not responding")
            self.handle_fail()
            return

        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.connect(self.saddr)
            self.create_events()
            self.write_stats_command()
            #self.set_timer()
        except Exception, e:
            Log.error("Unable to connect to the VBucketMigrator %s" %e)
            self.handle_fail()
        
    def write_stats_command(self):
        command = "stats\r\n"
        self.wbuf = command
        if not self.set_write():
            Log.error("Handle close called! write_stats")
            self.handle_response()

    def handle_write(self):
        try:
            if len(self.wbuf) > 0:
                sent = self.send(self.wbuf)
                self.wbuf = self.wbuf[sent:]
                if not self.set_read():
                    Log.error("Handle close called! set read")
                    self.handle_response()
            else:
                Log.error("Write on empty buffer!")
        except socket.error, why:
            Log.error("Exception when trying to write %s. Reconnecting and retrying %s" %(why, self.wbuf))
            ecode = why[0]
            if not (ecode == errno.EAGAIN and self.set_write()):
                Log.error("Read event error %s" % why)
                self.handle_response()
                return

    def set_write(self):
        status = True
        try:
            self.enable_write()
        except:
            Log.error("Error setting write event!")
            status = False
        return status

    def handle_response(self):
        self.stats= self.handle_stats_read()
        Log.error("Stats: %s" %self.stats)
        if self.stats is None:
            Log.error("No stats available for %s" %self.name)
            self.handle_fail()
        else:
            Log.error("Got the right response... set timer")
            self.set_timer()
            self.last_response_ts = int(time.time())
            if not self.read_callback is None:
                self.read_callback(self, response)

    def set_read(self):
        status = True
        try:
            self.enable_read()
        except:
            Log.error("Error setting read event!")
            status = False
        return status

    def handle_read(self):
        #Read response from vBucketMigrator
        #Parse stats and set in stats map
        #Update the timestamp
        response = None
        Log.error("In handle read")
        while True:
            try:
                self.rbuf += self.recv(self.buffer_size)
                Log.error(self.rbuf)
            except socket.error, why:
                Log.error("Read event error %s" % why)
                if why[0] == errno.EAGAIN:
                    if not self.set_read():
                        Log.error("Handle close called EAGAIN!")
                        self.handle_response()
                    break
                else:
                    self.handle_response()
                    Log.error("Handle close called!")
                    return
        Log.error("Got: %s" %self.rbuf)
                

    def handle_stats_read(self):
        #Lines: vb:0 rcvd:6 sent:6
        if len(self.rbuf) > 0:
            stats = {}
            msg = self.rbuf[:len(self.rbuf)]
            for line in self.rbuf.splitlines():
                m = re.match(r'vb:(\d+) rcvd:(\d+) sent:(\d+)', line)
                if (m != None):
                    vb = int(m.group(1))
                    rcvd = int(m.group(2))
                    sent = int(m.group(3))
                    stats[vb] = {"sent":sent, "rcvd": rcvd}
            self.rbuf = ""
            return stats
        return None

    def get_stats(self):
        stats = self.stats
        self.stats = None
        return stats

    def handle_fail(self):
        Log.critical("VBucketMigrator %s is not responding. Will try restarting" %self.name)
        self.destroy()

    def destroy(self):
        self.close()
