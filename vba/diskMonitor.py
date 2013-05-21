#!/usr/bin/env python
import sys
import os
import threading
import json
import asyncon

MEMBASE_LIB_DIR="/opt/membase/lib/python"
sys.path.append(MEMBASE_LIB_DIR)

import mc_bin_client
from logger import *

Log = getLogger()

class DiskMonitor(asyncon.AsynConDispatcher):
    TIMER = 5
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
        self.state = DiskMonitor.MONITOR

    def set_timer(self):
        self.timer_event.add(self.timer)

    def get_kvstores(self, mc = None):
        kvstores = None
        if mc is None:
            kvstores = self.vbs_mgr.get_kv_stats(self.host+":"+str(self.port))

        if kvstores is None:
            if mc is None:
                mc = mc_bin_client.MemcachedClient(self.host, self.port)
            kvstores = []
            try:
                kvstats = mc.stats(DiskMonitor.KVSTORE_STATS_STR)
                num_kvstores = int(kvstats[DiskMonitor.NUM_KVSTORE_KEY])
                num_kvstores += 1
                for i in range(1,num_kvstores):
                    key = "kvstore%d:status" %i
                    if kvstats[key] == DiskMonitor.ONLINE:
                        kvstores.append(i)
            except Exception, e:
                Log.error("Unable to get kvstats %s" %e)
                return
            finally:
                mc.close()
        self.kv_stores = kvstores
        self.state = DiskMonitor.MONITOR
        self.vbs_mgr.set_kvstores(self.kv_stores)
        return kvstores

    def get_commit_fails(self):
        commit_fail_key_prefix = "ep_item_commit_failed_"
        mc = mc_bin_client.MemcachedClient(self.host, self.port)
        try:
            stats = mc.stats()
            commit_fails = {}
            for kv in self.kv_stores:
                kv_key = commit_fail_key_prefix+str(kv-1)
                commit_fails[kv] = int(stats[kv_key])
            if self.commit_fails_cur is not None and (self.commit_fails is None):
                self.commit_fails = self.commit_fails_cur
            Log.debug("Setting commit fails cur to %s" %commit_fails)
            self.commit_fails_cur = commit_fails
        except Exception, e:
            Log.critical("Unable to get local stats %s" %e)
        finally:
            mc.close()

    def monitor(self):
        err_kv = []
        if self.commit_fails is not None and len(self.commit_fails) > 0:
            #Check if the fails diff is within the threshold
            for k,count in self.commit_fails_cur.items():
                count_old = self.commit_fails[k]
                if (count - count_old) >  DiskMonitor.COMMIT_FAILED_THRESHOLD:
                    err_kv.append(k)

        if len(err_kv) > 0:
            # Mark kvstore as offline and report
            Log.info("Sending failure to vbs for kv: %s" %err_kv)
            vb_stats = self.vbs_mgr.get_vb_stats(self.host+':'+str(self.port))
            if vb_stats is None:
                trep = threading.Thread(target=self.report_kvstores, args=(err_kv, None))
                trep.daemon = True
                trep.start()
            else:
                self.report_kvstores(err_kv, vb_stats)
        else:
            Log.debug("Disk is fine!")

        t = threading.Thread(target=self.get_commit_fails)
        t.daemon = True
        t.start()

    def report_kvstores(self, err_kv, vb_stats = None):
        #send error to vbs
        #mark vbuckets dead
        mc = mc_bin_client.MemcachedClient(self.host, self.port)
        try:
            if vb_stats is None:
                vbuckets_failed = self.get_failed_vbuckets(mc, err_kv)
            else:
                vbuckets_failed = self.get_failed_vbuckets_stats(vb_stats, err_kv)

            Log.debug("vBuckets failed: %s" %vbuckets_failed)
            self.vbs_mgr.send_error(json.dumps({"Cmd":"DEAD_VBUCKETS", "Status":"ERROR", "Vbuckets":{"Active":vbuckets_failed["active"], "Replica":vbuckets_failed["replica"]}, "DiskFailed":len(err_kv)}))
            #mark the vbuckets as dead
            Log.info("Marking kvstore(s) offline %s" %err_kv)
            self.vbs_mgr.migration_manager.set_local_vbucket_state(vbuckets_failed["active"], "dead")
            self.vbs_mgr.migration_manager.set_local_vbucket_state(vbuckets_failed["replica"], "dead")
            for kv in err_kv:
                mc.set_flush_param("kvstore_offline", str(kv))
            self.get_kvstores(mc)
        except Exception, e:
            Log.critical("Problem handling kvstores %s. %s" %(err_kv, e))
        finally:
            mc.close()

    def get_failed_vbuckets_stats(self, vb_stats, err_kv):
        active_list = []
        replica_list = []
        for vb,map in vb_stats.items():
            if (int(map["kvstore"])+1) in err_kv:
                if map["status"] == "active":
                    active_list.append(vb)
                else:
                    replica_list.append(vb)
        ret_map = {}
        ret_map["active"] = active_list
        ret_map["replica"] = replica_list
        return ret_map

    def get_failed_vbuckets(self, mc, err_kv):
        try:
            response = mc.stats(DiskMonitor.VBUCKET_STAT_STR)
            vmap = {}
            for k,v in response.items():
                vb = int(k.split("_")[1])
                v = "status "+v
                vals =v.split(" ")
                m={}
                for x in range(0,len(vals),2):
                    m[vals[x]] = vals[x+1]
                vmap[vb] = m
            ret_map = self.get_failed_vbuckets_stats(vmap, err_kv)
            return ret_map
        except Exception, e:
            Log.error("Error finding down vBuckets %s" %e)
            raise e

    def set_state(self, state):
        self.state = state

    def handle_state(self):
        #INIT, START, MONITOR, STOP
        if self.state == DiskMonitor.INIT:
            Log.debug("Initialize state")
        elif self.state == DiskMonitor.START:
            pass
            #self.start()
        elif self.state == DiskMonitor.MONITOR:
            self.monitor()
        elif self.state == DiskMonitor.STOP:
            Log.info("Set state to stop")
            self.destroy()
            return

        self.set_timer()
        
    def handle_timer(self):
        self.handle_state()
