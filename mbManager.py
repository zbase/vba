from logger import *
from asyncon import *
from message import *
from vbsManager import *

#Class to manage monitoring membase nodes
#Each monitoring agent is an instance of MembaseHandler
class MembaseManager:
    def __init__(self, as_mgr):
        self.as_mgr = as_mgr
        self.monitoring_host_list = []
        self.monitoring_agents = {}
        self.hb_interval = 30
        self.down_list = []

    def array_diff(self, a, b):
        b = set(b)
        a = set(a)
        d_a = [aa for aa in a if aa not in b]
        d_b = [bb for bb in b if bb not in a]
        return d_a, d_b

    def handle_config(self, config):
        self.hb_interval = config["HeartBeatTime"]
        server_list = config["Data"]["serverList"]

        servers_added,servers_removed = self.array_diff(server_list, self.monitoring_host_list)
        
        if len(servers_removed) > 0:
            Log.info("Will stop monitoring %s" %(servers_removed))
            for mb in servers_removed:
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
        ip,port = host.split(":",1)
        #Fail command handler and stats read handler set here
        params = {"ip":ip, "port":port, "failCallback":self.mb_fail_callback, 'mgr':as_mgr, "timeout":self.hb_interval, "type":MembaseHandler.STATS_COMMAND, "callback":self.mb_stats_callback}
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

    # When a membase node is down
    # Keep the host in a down list
    # Stop monitoring and report to vbs
    def mb_fail_callback(self, obj):
        self.down_list.append(obj.host)
        self.stop_monitoring(host)
    
    def get_down_list(self):
        return self.down_list
