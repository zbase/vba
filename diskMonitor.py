#!/usr/bin/env python
import sys
import os

MEMBASE_LIB_DIR="/opt/membase/lib/python"
sys.path.append(MEMBASE_LIB_DIR)

import mc_bin_client
from logger import *

Log = getLogger()

class DiskMonitor(asyncon.AsynConDispatcher):
    TIMER = 60
    VBUCKET_STAT_STR = "vbucket"
    KVSTORE_STATS_STR = "kvstore"
    NUM_KVSTORE_KEY = "num_kvstores"
    ONLINE = "online"
    COMMIT_FAILED_THRESHOLD = 5

    INIT, START, MONITOR, STOP = range(4)

    def __init__(self, vbs_mgr, as_mgr, host="127.0.0.1", port = 11211):
        self.vbs_mgr = vbs_mgr
        self.as_mgr = as_mgr
        self.timer = DiskMonitor.TIMER
        self.kv_stores = None
        self.cur_kv_stores = None
        self.commit_fails = None
        self.commit_fails_cur = None
        self.host = host
        self.port = port
        asyncon.AsynConDispatcher.__init__(self, None, self.timer, self.as_mgr)
        self.create_timer()
        self.set_timer()

    def set_timer(self):
        self.timer_event.add(self.timer)

    def start(self):
        t = threading.Thread(target=self.get_kvstores)
        t.daemon = True
        t.start()

    def get_kvstores(self):
        mc = mc_bin_client.MemcachedClient(self.host, self.port)
        kvstores = []
        try:
            kvstats = mc.stats(DiskMonitor.KVSTORE_STATS_STR)
            num_kvstores = int(kvstats[DiskMonitor.NUM_KVSTORE_KEY])
            num_kvstores += 1
            for i in range(1,num):
                key = "kvstore%d:status" %i
                if kvstats[key] == DiskMonitor.ONLINE:
                    kvstores.append(i)
        except Exception, e:
            Log.error("Unable to get kvstats %s" %e)
        finally:
            mc.close()
        self.kv_stores = kbstores
        self.state = DiskMonitor.MONITOR
        self.vbs_mgr.set_kvstores(self.kv_stores)
        return kvstores

    def get_commit_fails(self):
        commit_fail_key_prefix = "ep_item_commit_failed_"
        mc = mc_bin_client.MemcachedClient(self.host, self.port)
        try:
            stats = mc.stats()
            commit_fails = {}
            for kv in self.kv_stores.iteritems():
                kv_key = commit_fail_key_prefix+str(kv)
                commit_fails[kv] = stats[kv_key]
            if self.commit_fails_cur is not None and (self.commit_fails is None):
                self.commit_fails = self.commit_fails_cur
            self.commit_fails_cur = commit_fails
        except:
            Log.critical("Unable to get local stats")
        finally:
            mc.close()

    def monitor(self):
        err_kv = []
        if self.commit_fails is not None and len(self.commit_fails) > 0:
            #Check if the fails diff is within the threshold
            for k,count in self.commit_fails_cur:
                count_old = self.commit_fails[k]
                if (count - count_old) >  DiskManager.COMMIT_FAILED_THRESHOLD:
                    err_kv.append(k)

        if len(err_kv) > 0:
            # Mark kvstore as offline and report
            Log.info("Sending failure to vbs for kv: %s" %err_kv)
            trep = threading.Thread(target=self.report_kvstores, args=(err_kv))
            trep.daemon = True
            trep.start()

        t = threading.Thread(target=self.get_local_commit_fails)
        t.daemon = True
        t.start()
        Log.debug("Monitor err queues and send msg to vbs mgr")

    def report_kvstores(err_kv):
        #send error to vbs
        #mark vbuckets dead
        mc = mc_bin_client.MemcachedClient(self.host, self.port)
        try:
            vbuckets_failed = self.get_failed_vbuckets(mc, err_kv)
            self.vbs_manager.send_error(json.dumps({"Cmd":"DisksFailed", "Status":"ERROR", "Active":vbuckets_failed["active"], "Replica":vbuckets_failed["replica"]}))
            Log.info("Marking kvstore(s) offline %s" %err_kv)
            for kv in err_kv:
                mc.set_flush_param("kvstore_offline", str(kv))
        except Exception, e:
            Log.critical("Unable to send fail message to vbs for kvstores %s" %err_kv)
        finally:
            mc.close()

    def get_failed_vbuckets(self, err_kv, mc):
        response = mc.stats(DiskMonitor.VBUCKET_STAT_STR)
        active_list = []
        replica_list = []
        vmap = {}
        for k,v in response.items():
            vb = int(k.split("_")[1])
            v = "status "+v
            vals =v.split(" ")
            m={}
            for x in range(0,len(vals),2):
                m[vals[x]] = vals[x+1]
            vmap[vb] = m

        for vb,map in vmap.items():
            if int(map["kvstore"]) in err_kv:
                if map["status"] == "active":
                    active_list.append(vb)
                else:
                    replica_list.append(vb)
        ret_map = {}
        ret_map["active"] = active_list
        ret_map["replica"] = replica_list
        return ret_map

    def set_state(self, state):
        self.state = state

    def handle_state(self):
        #INIT, START, MONITOR, STOP
        if self.state == DiskMonitor.INIT:
            Log.debug("Initialize state")
        elif self.state = DiskMapper.START:
            self.start()
        elif self.state == DiskMapper.MONITOR:
            self.monitor()
        elif self.state == DiskMapper.STOP:
            Log.info("Set state to stop")
            self.destroy()
            return

        self.set_timer()
        
    def handle_timer(self):
        self.handle_state()
