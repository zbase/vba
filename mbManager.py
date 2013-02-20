from logger import *
from asyncon import *
from message import *
from vbsManager import *

#Class to manage monitoring membase nodes
#Each monitoring agent is an instance of MembaseHandler
class MembaseManager:
    def __init__(as_mgr):
        self.as_mgr = as_mgr
        self.monitoring_ip_list = []
        self.monitoring_agents = {}
        self.hb_interval = 30

    def array_diff(a, b):
        b = set(b)
        a = set(a)
        d_a = [aa for aa in a if aa not in b]
        d_b = [bb for bb in b if bb not in a]
        return d_a, d_b

    def change_config(config):
        self.hb_interval = resp["HeartBeatTime"]
        server_list = resp["Data"]["serverList"]

        servers_added,servers_removed = self.array_diff(server_list, self.monitoring_ip_list)
        
        if len(servers_removed) > 0:
            Log.info("Will stop monitoring %s" %(servers_removed))
            for mb in servers_removed:
                self.monitoring_agents[mb].destroy()
                del self.monitoring_agents[mb]

        if len(servers_added) > 0:
            Log.info("Start monitoring %s" %(servers_added))
            for mb in servers_added:
                self.start_monitoring(mb)

        self.monitoring_ip_list = server_list
