import string
import socket
import exceptions
import struct
import errno
import time

from connHandler import *
from mcsConstants import *
from logger import *
from asyncon import *

Log = getLogger()

class VBSHandler(AsynConDispatcher):
    def __init__(self, params):
        sock = None
        self.callback = None
        self.ip = None
        self.retryCount = TOT_RETRY_COUNT
        self.TOT_RETRY_COUNT = TOT_RETRY_COUNT
        self.failCallback = None
        self.failCallbackParams = None
        self.link = None
        self.addr = None
        self.port = None
        self.MIN_PACK_SIZE = 4
        timeout = 30.0
        port = None
        self._map = None
        mgr = None

        if(params.has_key('ip')):
            self.ip = params['ip']
        if(params.has_key('failCallbackParams')):
            self.failCallbackParams = params['failCallbackParams']
        if(params.has_key('callback')):
            self.callback = params['callback']
        if(params.has_key('callbackParams')):
            self.callbackParams = params['callbackParams']
        if(params.has_key('port')):
            port = params['port']
            self.port = str(port)
        if(params.has_key('map')):
            self._map = params['map']
        if(params.has_key('mgr')):
            mgr = params['mgr']
        else:
            Log.error("No manager set")
            return
        if(params.has_key("alive_msg")):
            self.alive_msg = params["alive_msg"]
        else:
            Log.error("No alive message set")
            return

        self.rbuf = ""
        self.wbuf = ""
        self.buffer_size = 4096 
        self.gotSize = False
        self.totSize = 0
        self.set_flags = False
        self.gotRes = False

        AsynConDispatcher.__init__(self, None, 30.0, mgr)

        if(params.has_key('addr')):
            self.addr = params['addr']

        #Creates non blocking socket
        if self.addr is not None:
            Log.debug("connecting to %s" %(str(self.addr)))
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connect(self.addr)
            self.ip,self.port = self.socket.getsockname()
        else:
            Log.info("No ip or port specified")
            return
        self.enable_timeout()
        self.set_read()

    def set_timeout(self, timeout):
        self.timeout = timeout

    def handle_error(self):
        Log.debug("Handle error for %s" %(self.ip))
        self.failCallback(*self.failCallbackParams)
        self.destroy()

    def handle_connect(self):
        self.connected = True
        self.set_read()

    def isConnected(self):
        return self.connected

    def destroy(self):
        if(self.connected == True):
            self.connected = False
            self.close()

    def set_read(self):
        status = True
        try:
            self.enable_read()
        except:
            Log.error("Error setting read event!")
            status = False
            self.handle_error()
        return status

    def handle_read(self):
        #try:
            while True:
                try:
                    self.rbuf += self.recv(self.buffer_size)
                    Log.debug(self.rbuf)
                except socket.error, why:
                    ecode = why[0]
                    if ecode == errno.EAGAIN:
                        break
                    else:
                        Log.error("Read event error %s" % why)
                        self.handle_error()
                        return
            if not self.set_read():
                Log.debug("Error setting read event for %s. Will try reconnecting/switch to local hbm" %self.ip)
                return
            self.gotRes = True
            while len(self.rbuf) > 0:
                if not self.gotSize:
                    if len(self.rbuf) > self.MIN_PACK_SIZE:
                        self.totSize, = struct.unpack("!I", self.rbuf[:self.MIN_PACK_SIZE])
                        #self.totSize = (self.rbuf[:self.MIN_PACK_SIZE])
                        Log.info("Size: %s", str(self.totSize))
                        self.totSize += self.MIN_PACK_SIZE
                        self.gotSize = True
                if self.gotSize and len(self.rbuf) < self.totSize:
                    Log.info("returning due to size")
                    return
                else:
                    Log.debug("Callback for: %s" %self.rbuf[self.MIN_PACK_SIZE:self.totSize])
                    self.callback(self, self.rbuf[self.MIN_PACK_SIZE:self.totSize])
                    self.gotSize = False
                    self.rbuf = self.rbuf[self.totSize:]
        #except:
        #    Log.debug("Error setting read event for %s. Will try reconnecting/switch to local hbm" %self.ip)
        #    self.handle_error()
        #    return

    def handle_timeout(self):
        if not self.connected:
            Log.info("Not connected!")
            self.handle_error()
        else:
            try:
                self.send_alive()
                self.enable_timeout()
            except Exception, why:
                Log.error("Exception: %s" %(str(why)))
                self.handle_error()
            self.retryCount = TOT_RETRY_COUNT

    def send_alive(self):
        self.writeData(self.alive_msg)

    def writeData(self, data, callback=None, close=None):
        Log.debug("In write data.... %s" %(data))
        if not data:
            Log.info("data is null")
            return
        self.closeSock = close
        self.wbuf += data
        if callback:
            self.callback = callback
        try:
            self.enable_write()
        except:
            Log.error("Error setting write event")
            self.handle_error()

    def writable(self):
        return len(self.wbuf) > 0

    def handle_write(self):
        try:
            if self.link:
                Log.info("client sending %s" % self.wbuf)
            else:
                Log.info("server sending %s" % self.wbuf)

            if len(self.wbuf) > 0:
                sent = self.send(self.wbuf)
                self.wbuf = self.wbuf[sent:]
            else:
                Log.debug("Write on empty buffer!")

            #self.enable_read()
        except socket.error, why:
            Log.error("Exception when trying to write %s. Reconnecting and retrying %s" %(why, self.wbuf))
            if self.retryCount > 0:
                self.reconnect()
                self.enable_write()
            else:
                Log.error("Retry over..." )
                self.handle_error()
                return
        except:
            Log.error("Exception in handle write!")
            self.handle_error() 
            return


    def handle_close(self):
        self.destroy()
