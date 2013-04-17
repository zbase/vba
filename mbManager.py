import asyncon
import threading
from membaseHandler import *
from message import *
from vbsManager import *
from logger import *

import utils

#Class to manage monitoring membase nodes
#Each monitoring agent is an instance of MembaseHandler
class MembaseManager(AsynConDispatcher):
    INIT, CONFIG, MONITOR, STOP = range(4)
    LOCAL = "127.0.0.1:11211"

    def __init__(self, vbs_mgr):
        self.as_mgr = None
        self.monitoring_host_list = []
        self.monitoring_agents = {}
        self.hb_interval = 30
        self.down_list = []
        self.state = MembaseManager.INIT
        self.vbs_mgr = vbs_mgr
        self.as_mgr = asyncon.AsynCon()
        self.timer = 5
        self.config = None
        asyncon.AsynConDispatcher.__init__(self, None, self.timer, self.as_mgr)
        self.create_timer()
        self.set_timer()

    def handle_config(self):
        if (self.config.has_key('HeartBeatTime')):
            self.hb_interval = self.config['HeartBeatTime']
            self.timer = self.hb_interval

        config_data = self.config.get('Data')
        if (config_data == None or len(config_data) == 0):
            Log.warning('VBucket map missing in config')
            return False

        Log.debug('New config from VBS: %s', str(config_data))
        server_list = []
        for row in config_data:
            server_list.append(row['Destination'])
        
        self.hb_interval = self.config["HeartBeatTime"]

        servers_added = utils.diff(server_list, self.monitoring_host_list)
        servers_removed = utils.diff(self.monitoring_host_list, server_list)
        
        if len(servers_removed) > 0:
            Log.info("Will stop monitoring %s" %(servers_removed))
            for mb in servers_removed:
                if mb == MembaseManager.LOCAL:
                    continue
                self.stop_monitoring(mb)
                # remove from down list
                if mb in self.down_list:
                    self.down_list.remove(mb)

        if len(servers_added) > 0:
            Log.info("Start monitoring %s" %(servers_added))
            for mb in servers_added:
                self.start_monitoring(mb)

        self.monitoring_host_list = server_list

    def start_monitoring(self, host):
        #Add ip to monitoring list
        #create a new MembaseHandler instance
        local = (host == MembaseManager.LOCAL)
        ip,port = host.split(":",1)
        #Fail command handler and stats read handler set here
        params = {"ip":ip, "port":port, "failCallback":self.mb_fail_callback, 'mgr':self.as_mgr, "timeout":self.hb_interval, "type":MembaseHandler.KV_STATS_MONITORING, "callback":self.mb_stats_callback, "mb_mgr":self, "local":local}
        mb = MembaseHandler(params)
        self.monitoring_host_list.append(host)
        self.monitoring_agents[host] = {"agent":mb}
        mb.send_stats()

    # Stop monitoring ip
    # Destroy the connection and remove the host
    def stop_monitoring(self, host):
        if host in self.monitoring_agents:
            self.monitoring_agents[host]["agent"].destroy()
            del self.monitoring_agents[host]


    # Maintain stats for the node
    def mb_stats_callback(self, obj, response):
        self.monitoring_agents[obj.host]["stats"] = response

    def get_vb_stats(self, host):
        if self.monitoring_agents.has_key(host):
            return self.monitoring_agents[host]["agent"].vb_stats
        return None
    
    def get_kv_stats(self, host):
        if self.monitoring_agents.has_key(host):
            return self.monitoring_agents[host]["agent"].kv_stats
        return None

    # When a membase node is down
    # Keep the host in a down list
    # Stop monitoring and report to vbs
    def mb_fail_callback(self, obj):
        self.down_list.append(obj.host)
        self.stop_monitoring(obj.host)
        self.vbs_mgr.report_down_node(obj.host)

    def report_stats(self, stats_str):
        self.vbs_mgr.send_message(stats_str)
    
    def get_down_list(self):
        return self.down_list

    def set_timer(self):
        self.timer_event.add(self.timer)

    def set_config(self, config):
        self.config = config
        self.state = MembaseManager.CONFIG

    def monitor(self):
        Log.debug("Monitoring")

    def stop_all_monitoring(self):
        for k in self.monitoring_agents.keys():
            self.monitoring_agents[k].destroy()
            Log.info("Stopping monitoring %s" %k)
        self.monitoring_agents = {}

    def handle_init(self):
        if not self.monitoring_agents.has_key(MembaseManager.LOCAL):
            Log.info("Starting local membase connection")
            self.start_monitoring(MembaseManager.LOCAL)

    def handle_states(self):
        if self.state == MembaseManager.INIT:
            self.handle_init()
        elif self.state == MembaseManager.CONFIG:
            Log.debug("Handle config")
            self.handle_config()
        elif self.state == MembaseManager.MONITOR:
            self.monitor()
        elif self.state == MembaseManager.STOP:
            self.stop_monitoring()
        elif self.state == MembaseManager.END:
            Log.debug("Nothing to do!")

    def handle_timer(self):
        self.handle_states()
        self.set_timer()

    def start_monitor_loop(self):
        self.set_timer()
        self.as_mgr.loop()

    def run(self):
        t = threading.Thread(target=self.start_monitor_loop)
        t.daemon = True
        t.start()
