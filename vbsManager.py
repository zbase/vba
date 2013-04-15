import json
from logger import *
from vbsHandler import *
from message import *
import fcntl
import array
import struct
import socket
import platform

# global constants.  If you don't like 'em here,
# move 'em inside the function definition.
SIOCGIFCONF = 0x8912
MAXBYTES = 8096

Log = getLogger()

class VBSManager:
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
        Log.info("Server name: %s : %d" %(self.VBS_SERVER_NAME, self.VBS_SERVER_PORT))
        self.connect()

    def set_membase_manager(self, mb_mgr):
        self.membase_manager = mb_mgr

    def get_vb_stats(self, host):
        return self.membase_manager.get_vb_stats()

    def get_kv_stats(self, host):
        return self.membase_manager.get_kv_stats()

    def set_kvstores(self, kvstores):
        if self.kvstores != kvstores and self.init_done:
            message = {"Cmd":"CapacityUpdate ", "DiskAlive":len(kvstores)}
            self.send_message(json.dumps(message))
        self.kvstores = kvstores

    def set_migration_manager(self, mg_mgr):
        self.migration_manager = mg_mgr

    def vbs_enabled(self):
        return self.vbs_config

    def connect(self):
        alive_msg_json = json.dumps({"Cmd":"ALIVE"})
        alive_msg = VBSMessage.getMsg(alive_msg_json)
        Log.info("VBS SERVER ADDR: %s" %(str(self.VBS_SERVER_ADDR)))
        fcb_params = self.VBS_SERVER_ADDR
        params = {"addr":self.VBS_SERVER_ADDR, "port":self.VBS_SERVER_PORT, "failCallbackParams": fcb_params,"failCallback":self.error_handler, "callback": self.response_handler, "mgr":self.as_mgr, "alive_msg":alive_msg}
        self.vbs_con = VBSHandler(params)

    def error_handler(self, ip, port):
        Log.info("Re-establishing the VBS connection %s %d" %(ip,port))
        self.vbs_con.destroy()
        self.connect()

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
        #resp_str = json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":err_details})
        self.send_message(error)

    def report_down_node(self, ip):
        self.send_message(json.dumps({"Cmd":"FAIL", "Desptination":ip}))

    def send_message(self, msg):
        self.vbs_con.writeData(VBSMessage.getMsg(msg))
