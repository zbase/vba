import json
from logger import *
from vbsHandler import *
from message import *
import fcntl
import array
import struct
import socket
import platform
import Queue
import asyncon

# global constants.  If you don't like 'em here,
# move 'em inside the function definition.
SIOCGIFCONF = 0x8912
MAXBYTES = 8096

Log = getLogger()

class VBSManager(asyncon.AsynConDispatcher):
    CONFIG  = "CONFIG"
    INIT    = "INIT"

    def __init__(self, vbs_ip, vbs_port, as_mgr, mig_mgr=None, mb_mgr=None, errqueue = None):
        self.as_mgr = as_mgr
        self.VBS_SERVER_NAME = vbs_ip
        self.VBS_SERVER_PORT = vbs_port
        self.VBS_SERVER_ADDR = (self.VBS_SERVER_NAME, self.VBS_SERVER_PORT)
        self.migration_manager = mig_mgr
        self.membase_manager = mb_mgr
        self.errqueue = errqueue
        self.kvstores = None
        self.init_done = False
        self.timer = 10
        self.error = True
        self.msg_queue = Queue.Queue()
        Log.info("Server name: %s : %d" %(self.VBS_SERVER_NAME, self.VBS_SERVER_PORT))
        asyncon.AsynConDispatcher.__init__(self, None, self.timer, self.as_mgr)
        self.create_timer()
        self.set_timer()
        #self.connect()

    def set_timer(self):
        self.timer_event.add(self.timer)

    def set_membase_manager(self, mb_mgr):
        self.membase_manager = mb_mgr

    def get_vb_stats(self, host):
        return self.membase_manager.get_vb_stats(host)

    def get_kv_stats(self, host):
        return self.membase_manager.get_kv_stats(host)

    def set_kvstores(self, kvstores):
        if self.kvstores != kvstores and self.init_done:
            message = {"Cmd":"CapacityUpdate ", "DiskAlive":len(kvstores)}
            self.send_message(json.dumps(message))
        self.kvstores = kvstores

    def set_migration_manager(self, mg_mgr):
        self.migration_manager = mg_mgr

    def vbs_enabled(self):
        return self.vbs_config

    def handle_timer(self):
        if self.error:
            self.connect()
            self.error = False

    def connect(self):
        Log.debug("Connecting to VBS")
        alive_msg_json = json.dumps({"Cmd":"ALIVE"})
        alive_msg = VBSMessage.getMsg(alive_msg_json)
        Log.info("VBS SERVER ADDR: %s" %(str(self.VBS_SERVER_ADDR)))
        fcb_params = self.VBS_SERVER_ADDR
        params = {"addr":self.VBS_SERVER_ADDR, "port":self.VBS_SERVER_PORT, "failCallbackParams": fcb_params,"failCallback":self.error_handler, "callback": self.response_handler, "mgr":self.as_mgr, "alive_msg":alive_msg}
        Log.debug("New vbs connection being created")
        self.vbs_con = VBSHandler(params)
        Log.debug("Conn created")

    def error_handler(self, ip, port):
        Log.info("Re-establishing the VBS connection %s %d" %(ip,port))
        self.error = True
        self.set_timer()
        #self.vbs_con.destroy()

    def response_handler(self, obj, resp_str):
        Log.info("Response handler with %s" %resp_str)
        try:
            resp = json.loads(resp_str)
        except Exception, why:
            Log.error("Error parsing json: '%s'" %str(why))
            return
        if resp is None:
            Log.error("No response from VBS")
            return
        if resp["Cmd"] == "CONFIG":
            Log.debug("Response handler with %s" %resp_str)
            self.migration_manager.set_config(resp)
            self.membase_manager.set_config(resp)
            if resp.has_key("HeartBeatTime"):
                hb_interval = resp["HeartBeatTime"]
                self.vbs_con.set_timeout(hb_interval)
        elif resp["Cmd"] == "INIT":
            self.handle_init()

    def send_ok(self):
        resp_str = json.dumps({"Cmd":"Config", "Status":"OK"})
        self.send_message(resp_str)

    def handle_init(self):
        disk_count = 0
        if self.kvstores is not None:
            disk_count = len(self.kvstores)
        resp_str = json.dumps({"Agent":"VBA", "Capacity":disk_count})
        self.send_message(resp_str)
        self.init_done = True

    def send_error(self, error):
        self.send_message(error)

    def report_down_node(self, host):
        self.send_message(json.dumps({"Cmd":"FAIL", "Destination":ip}))

    def send_message(self, msg):
        try:
            self.vbs_con.msg_queue.put(VBSMessage.getMsg(msg))
        except Exception, e:
            Log.error("Unable to send message to VBS %s --- %s" %(msg, e))

