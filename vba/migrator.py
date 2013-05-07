#!/usr/bin/env python
import threading
import sys
import os
import signal
import zlib
import time
import json

MEMBASE_LIB_DIR="/opt/membase/lib/python"
sys.path.append(MEMBASE_LIB_DIR)

import mc_bin_client
import utils
import asyncon
import subprocess
import mbMigratorHandler 
from logger import *

Log = getLogger()

class Migrator(asyncon.AsynConDispatcher):
    CHECKPOINT_STATS_STR = "checkpoint"
    VBUCKET_STATS_STR = "vbucket"
    KVSTORE_STATS_STR = "kvstore"
    VBUCKET_MIGRATOR_PATH = "/opt/membase/bin/vbucketmigrator"
    INIT, START, CHECK_TAP, TAP_REGISTER, RUN, MONITOR, ERROR, RESTART, FAIL, CHECK_TRANSFER_COMPLETE, TRANSFER_COMPLETE, STOP, END = range(13)
    RETRY_COUNT = 3
    INIT_TIMER = 2
    MONITOR_TIMER = 10
    VBM_TIMER = 5 
    VBM_STATS_SOCK = "/var/tmp/vbs/vbm.sock"
    MAX_MONITOR_INTERVAL = 30
    MAX_ITEM_THRESHOLD = 500

    def __init__(self, key, migration_mgr, config, mb_mgr, as_mgr):
        self.migration_mgr = migration_mgr
        self.mb_mgr = mb_mgr
        self.key = key 
        self.vbmp = None
        self.state = Migrator.INIT
        self.tap_registered = False 
        self.as_mgr = as_mgr
        self.source_vb_set = False
        self.dest_vb_set = False
        self.retry_count = Migrator.RETRY_COUNT
        self.timer = Migrator.INIT_TIMER
        self.vbm_monitor = None
        self.migrator_stats = None
        self.config = config
        self.vb_stats = None
        self.vb_dest_stats = None
        self.transfer = False
        self.monitor_ts = time.time()
        asyncon.AsynConDispatcher.__init__(self, None, self.timer, self.as_mgr)
        self.create_timer()
        self.set_timer()
        self.start()

    def set_timer(self):
        self.timer_event.add(self.timer)

    def start(self):
        #self.config = self.migration_mgr.get(self.key)
        if self.config == None:
            Log.info("config is null for migrator")
            self.state = Migrator.END
            return
        source = self.config.get('source')
        dest = self.config.get('destination')
        vblist = self.config.get('vblist')
        self.transfer = self.config.get('transfer')
        if  self.transfer is None:
            self.transfer = False

        if self.source_vb_set and self.dest_vb_set:
            self.state = Migrator.CHECK_TAP
            return
        if not self.source_vb_set:
            t = threading.Thread(target=self.init_vbucket, args=(source, vblist, "active"))
            t.daemon = True
            t.start()
        if not self.dest_vb_set and not self.transfer:
            t = threading.Thread(target=self.init_vbucket, args=(dest, vblist, "replica"))
            t.daemon = True
            t.start()

    def init_vbucket(self, host, vblist, state):
        if self.set_vbucket_state(host, vblist, state) == 0:
            if state == "active":
                self.source_vb_set = True
            else:
                self.dest_vb_set = True
            self.retry_count = Migrator.RETRY_COUNT
        else:
            #Handle error
            Log.error("Could not mark vbuckets as %s for %s" %(state, host))
            self.retry_count -= 1

    def set_vbucket_state(self, hostport, vblist, state):
        if hostport == "":
            return 0
        host,port = hostport.split(":")
        mc = mc_bin_client.MemcachedClient(host, int(port))
        Log.debug("setting %s for %s" %(hostport, state))
        try:
            for vb in vblist:
                op, cas, data = self.setvb(mc, str(vb), state)
                Log.debug("setting %s for %s vbucket %d" %(hostport, state, vb))
                print op
        except Exception, e:
            Log.error("Could not setup vBuckets %s" %e)
            return 1
        finally:
            mc.close()
        return 0

    def check_tap(self):
        if self.tap_registered:
            self.state = Migrator.RUN
        else:
            t = threading.Thread(target=self.check_tap_registered)
            t.daemon = True
            t.start()


    def getTapName(self):
        source = self.config.get('source')
        dest = self.config.get('destination')
        tap_name = "repli-" + ("%X" % zlib.crc32(self.key))
        Log.info("tap name %s %s %s" % (source, dest, tap_name))    
        #tap_name = "repli-" + (host+dest2)
        return tap_name

    def check_tap_registered(self):
        source = self.config.get('source')
        vblist = self.config.get('vblist')
        host,port = source.split(":")
        
        mc = mc_bin_client.MemcachedClient(host, int(port))
        try:
            response = mc.stats(Migrator.CHECKPOINT_STATS_STR)
            #Sample vb_0:cursor_checkpoint_id:eq_tapq:repli--47FD90C2
            #tap_name = "repli-" + ("%X" % zlib.crc32(self.key))
            tap_name = self.getTapName()
            if response is not None and len(response) > 0:
                found = False
                for vb in vblist:
                    key = "vb_%d:cursor_checkpoint_id:eq_tapq:%s" %(vb, tap_name)
                    Log.debug("Checking key: %s" %key)
                    if response.has_key(key):
                        found = True
                        break
                # Tap registered. Start VBM
                if found:
                    self.state = Migrator.RUN

            # Register tap
            if self.state != Migrator.RUN:
                self.state = Migrator.TAP_REGISTER

            self.retry_count = Migrator.RETRY_COUNT
        except Exception, e:
            Log.error("Error checking tap registration %s" %e)
            self.retry_count -= 1
        finally:
            mc.close()

    def register_tap(self):
        if self.state == Migrator.TAP_REGISTER:
            t = threading.Thread(target=self.call_tap_register)
            t.daemon = True
            t.start()

    def call_tap_register(self): 
        source = self.config.get('source')
        #tapname = "repli-" + ("%X" % zlib.crc32(self.key))
        tapname = self.getTapName()
        checkpoints = self.config.get('CheckPoints')
        #TODO: Register checkpoints per vbucket
        Log.info("TODO: Register checkpoints per tap. Defaulting to -1 for all")
        if utils.register_tap_name(source.split(":"), tapname) == 0:
            self.state = Migrator.RUN
            self.retry_count = Migrator.RETRY_COUNT
        else:
            self.retry_count -= 1
            Log.error("Unable to register tap")

    def run_migrator(self):
        print "Starting migrator process"
        source = self.config.get('source')
        dest = self.config.get('destination')
        vblist = self.config.get('vblist')
        interface = self.config.get('interface')
        #tapname = "repli-" + ("%X" % zlib.crc32(self.key))
        tapname = self.getTapName()

        vblist_str = ",".join(str(vb) for vb in vblist)
        vbmp_arr = ["sudo", Migrator.VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", ]
        if interface != '':
            vbmp_arr.append("-i")
            vbmp_arr.append(interface)

        if self.transfer:
            #-t for transfer vbuckets
            vbmp_arr.append("-t")

        # Starting vBucketMigrator
        #self.vbmp = subprocess.Popen(["sudo", Migrator.VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", "-i", interface, "-v", "-r"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        self.vbmp = subprocess.Popen(vbmp_arr, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        Log.info('Started VBM with pid %d interface %s', self.vbmp.pid, interface)
        Log.info("Switching to monitor mode for %s" %self.key)
        self.state = Migrator.MONITOR

    def setvb(self, mc, vbid, vbstate, username=None, password=""):
        if not username is None:
            mc.sasl_auth_plain(username, password)
        return mc.set_vbucket_state(int(vbid), vbstate)

    def setup_vbuckets(self):
        source = self.config.get('source')
        dest = self.config.get('destination')
        vblist = self.config.get('vblist')
        host,port = source.split(":")
        mc = mc_bin_client.MemcachedClient(host, int(port))
        dmc = None

        #TODO: Handle error from setvb
        if dest != '':
            dhost, dport = dest.split(":")
            dmc = mc_bin_client.MemcachedClient(dhost, int(dport))
        try:
            for vb in vblist:
                op, cas, data = self.setvb(mc, str(vb),  "active")

                if dest != '':
                    op, cas, data = self.setvb(dmc, str(vb), "replica")
            Log.info("Set %s and %s" %(source, dest))
        except Exception, e:
            Log.error("Could not setup vBuckets %s" %e)
            return 1
        finally:
            mc.close()
            if not dmc is None:
                dmc.close()
        return 0

    def handle_fail(self):
        Log.debug("Handle migrator fail")

    def set_change_config(self, config):
        self.config = config
        self.restart_vbm(Migrator.INIT)

    def monitor(self):
        Log.debug("Monitor vbm")
        source = self.config.get('source')
        dest = self.config.get('destination')

        if dest == "":
            return

        if self.vbm_monitor is None:
            (host, port) = dest.split(':')
            (s,p)  = source.split(':')
            vbm_stats_sock = Migrator.VBM_STATS_SOCK + "." + host + "." + s
            Log.debug("Sock: %s" %vbm_stats_sock)
            params={"addr":vbm_stats_sock, "name":dest, "timeout":Migrator.VBM_TIMER, "mgr":self.as_mgr}
            self.vbm_monitor = mbMigratorHandler. MBMigratorHandler(params)
        else:
            stats = self.vbm_monitor.get_stats()
            if not stats is None and self.is_vbm_progressing(stats):
                Log.debug("VBM progress is fine")
            elif not stats:
                Log.error("VBM %s is not running. Will try restarting" %dest)
                if self.vbmp:
                    (statout, staterr) = self.vbmp.communicate() 
                    Log.info("out %s err %s" % (statout, staterr))
                if not self.transfer:
                    self.state = Migrator.RESTART
                    if self.retry_count > 0:
                        self.retry_count -= 1
                    else:
                        msg = {"Cmd":"REPLICATION_FAIL", "Destination":dest}
                        vb_stats = self.migration_mgr.vbs_manager.send_message(json.dumps(msg))
                        self.retry_count = Migrator.RETRY_COUNT
                else:
                    self.state = Migrator.CHECK_TRANSFER_COMPLETE
            else:
                Log.info("VBM %s is not progressing" %dest)
        self.timer = Migrator.MONITOR_TIMER
        self.monitor_ts = time.time()

    def is_alive(self):
        cur_time = time.time()
        if cur_time - self.monitor_ts > Migrator.MAX_MONITOR_INTERVAL:
            return False
        else:
            return True

    def is_vbm_progressing(self, stats):
        ret = False
        if not self.migrator_stats is None:
            for vb,new_stats in stats.items():
                if not self.migrator_stats.has_key(vb):
                    ret = True
                    Log.error("Unknown vBucket %s" %str(vb))
                    break

                old_stats = self.migrator_stats[vb]
                if old_stats['sent'] != new_stats['sent'] or old_stats['rcvd'] != new_stats['rcvd']:
                    ret = True
                    break
        self.migrator_stats = stats

    def restart_vbm(self, state):
        if not self.vbm_monitor is None:
            self.vbm_monitor.destroy()
            self.vbm_monitor = None
        dest = self.config.get('destination')
        Log.info("Attempting to restart the vBucketMigrator %s" %dest)
        if self.vbmp and self.vbmp.poll() == None:
            os.kill(self.vbmp.pid, signal.SIGTERM)
        else:
            Log.info("No pid to restart")
        self.state = state
        self.timer = Migrator.INIT_TIMER

    def kill_migrator(self):
        if not self.vbm_monitor is None:
            self.vbm_monitor.destroy()
            self.vbm_monitor = None
        if self.config == None:
            Log.info("config is null")
        else:    
            vblist = self.config.get('vblist')
            source = self.config.get('source')
            dest = self.config.get('destination')
            self.set_vbucket_state(source, vblist, "dead")
            self.set_vbucket_state(dest, vblist, "dead")
            # Stop - kill the VBM and return
            if self.vbmp and self.vbmp.poll() == None:
                Log.info('Stop request for key %s, will kill the vbucket migrator (pid %d)', self.key, self.vbmp.pid)
                os.kill(self.vbmp.pid, signal.SIGTERM)
                Log.info("killing the migrator")        
        self.state = Migrator.END

    def stop_migrator(self):
        self.state = Migrator.STOP

    def check_transfer_complete(self):
        #Check the vb item counts!
        ret = self.is_transfer_complete()
        if ret is not None and len(ret) == 0:
            self.state = Migrator.TRANSFER_COMPLETE
        elif ret is not None:
            #Err vbs available
            dest = self.config.get('destination')
            if self.retry_count > 0:
                Log.info("Unable to complete the transfer. Restarting vBucketMigrator for %s" %dest)
                self.retry_count -= 1
                self.state = Migrator.RESTART
            else:
                Log.error("Failed to transfer the vBuckets. Reporting to VBS")
                self.handle_transfer_fail(ret)

    def is_transfer_complete(self):
        dest = self.config.get('destination')
        self.local_item_counts = self.get_items("127.0.0.1:11211")
        self.remote_item_count = self.get_items(dest)

        if self.local_item_counts is None:
            t = threading.Thread(target=self.get_vb_items)
            t.daemon = True
            t.start()

        if self.remote_item_count is None:
            t = threading.Thread(target=self.get_vb_items, args=(False))
            t.daemon = True
            t.start()

        if (self.local_item_count is not None) and (self.remote_item_count is not None):
            err_vbs = []
            for vb,val in self.local_item_count:
                if (val - self.remote_item_count[vb]) > Migrator.MAX_ITEM_THRESHOLD:
                    err_vbs.append(vb)
            return err_vbs
        return None

    #Get item counts for the vBuckets from local and remote machines
    def get_vb_items(self, local=True):
        host = "127.0.0.1"
        port = 11211
        vblist = self.config.get('vblist')
        if not local:
            dest = self.config.get('destination')
            host,port = dest.split(":")
            port = int(port)
         
        mc = mc_bin_client.MemcachedClient(host, port)
        Log.debug("setting %s for %s" %(hostport, state))
        stats_map = {}
        try:
            stats = mc.stats(VBUCKET_STATS_STR)
            for vb in vblist: 
                k = "vb_%d" %vb
                val = stats[k].split(" ")
                count = int(val[3])
                stats_map[vb] = count

            if local:
                self.local_item_count = stats_map
            else:
                self.remote_item_count = stats_map
        except Exception, e:
            Log.error("Could not get vbucket stats for %s. %s" %(host, e))
            return 1
        finally:
            mc.close()

    def get_items(self, host):
        vblist = self.config.get('vblist')
        vb_stats = self.migration_mgr.vbs_manager.get_vb_stats(host)
        stats_map = {}
        for vb in vblist:
            stats_map[vb] = vb_stats[vb]['curr_items']
        return stats_map

    def handle_transfer_complete(self):
        self.retry_count = Migrator.RETRY_COUNT
        dest = self.config.get('destination')
        vblist = self.config.get('vblist')
        Log.info("Transfer is complete for vbuckets: %s" %vblist)
        response = {"Cmd":"TRANSFER_DONE", "Status":"OK", "Destination": dest, Vbuckets:{"Active":vblist, "Replica":[]}}
        self.migration_mgr.vbs_manager.send_message(json.dumps(response))
        self.stop_migrator()

    def handle_transfer_fail(self, vblist):
        dest = self.config.get('destination')
        Log.info("Transfer is complete for vbuckets: %s" %vblist)
        response = {"Cmd":"TRANSFER_DONE", "Status":"ERROR", "Destination": dest, Vbuckets:{"Active":vblist, "Replica":[]}}
        self.migration_mgr.vbs_manager.send_message(json.dumps(response))
        self.stop_migrator()

    def sleep(self):
        Log.info("sleeping migrator")
        pass

    def handle_states(self):
        Log.info("Current state: %d" %self.state)
        if self.state == Migrator.INIT:
            # Mark vBuckets as active/replica
            self.start()
        elif self.state == Migrator.CHECK_TAP:
            self.check_tap()
        elif self.state == Migrator.TAP_REGISTER:
            # Register taps
            self.register_tap()
        elif self.state == Migrator.RUN:
            # Start vBucketMigrator processes
            self.run_migrator()
        elif self.state == Migrator.MONITOR:
            # Check replication status
            self.monitor()
        elif self.state == Migrator.STOP:
            # Stop monitoring
            self.kill_migrator()
        elif self.state == Migrator.FAIL:
            # Handle failure
            self.handle_fail()
        elif self.state == Migrator.RESTART:
            # Handle failure
            self.restart_vbm(Migrator.RUN)
        #elif self.state == Migrator.CHANGE_CONFIG:
            # Handle config change
         #   self.change_config()
        elif self.state == Migrator.CHECK_TRANSFER_COMPLETE:
            self.check_transfer_complete()
        elif self.state == Migrator.TRANSFER_COMPLETE:
            self.handle_transfer_complete()

    def handle_timer(self):
        Log.debug("Handle timer! %d" %self.state)
        self.handle_states()
        if self.state != Migrator.END:
            self.set_timer()

