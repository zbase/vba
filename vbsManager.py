import json
from logger import *
from vbsHandler import *
from message import *

Log = getLogger()

class VBSManager:
    CONFIG  = "CONFIG"
    INIT    = "INIT"

    def __init__(self, vbs_ip, vbs_port, as_mgr, mig_mgr=None, mb_mgr=None):
        self.as_mgr = as_mgr
        self.VBS_SERVER_NAME = vbs_ip
        self.VBS_SERVER_PORT = vbs_port
        self.VBS_SERVER_ADDR = (self.VBS_SERVER_NAME, self.VBS_SERVER_PORT)
        self.migration_manager = mig_mgr
        self.membase_manager = mb_mgr
        Log.info("Server name: %s : %d" %(self.VBS_SERVER_NAME, self.VBS_SERVER_PORT))
        self.connect()

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
            print("Response handler with %s" %resp_str)
            valid_config = mm.handle_new_config(resp, ifaces)
            vbs_sock.send_data(config_resp_str, len(config_resp_str))
            if valid_config:
                hb_interval = resp["HeartBeatTime"]
                self.vbs_con.set_timeout(hb_interval)
                self.membase_manager.handle_config(resp)

                self.send_ok()
            else:
                self.send_error()
            """data_obj = resp["Data"]
            updatedVolatileList = data_obj["serverList"]
            newVolatileList = [ip for ip in updatedVolatileList if ip != "0.0.0.0:11211"]
            hb_interval = resp["HeartBeatTime"]
            self.vbs_con.set_timeout(hb_interval)
            self.handle_config(newVolatileList)"""
        elif resp["Cmd"] == "INIT":
            self.handle_init()

    def send_ok(self):
        resp_str = json.dumps({"Status":"OK"})
        self.send_message(resp_str)

    def handle_init(self):
        resp_str = json.dumps({"Agent":"VBA"})
        self.send_message(resp_str)

    def send_error(self):
        resp_str = json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":err_details})
        self.send_message(resp_str)

    def report_down_node(self, ip):
        self.send_message(json.dumps({"Cmd":"FAIL", "Server":ip}))

    def send_message(self, msg):
        self.vbs_con.writeData(VBSMessage.getMsg(msg))

    def handle_config(self, volatileIps):
        print "Got change config: %s" %volatileIps
