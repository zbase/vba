#!/usr/bin/env python
import threading
import sys
import os
import signal
import zlib
import time

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
    #VBUCKET_MIGRATOR_PATH = "/opt/membase/bin/vbucketmigrator"
    VBUCKET_MIGRATOR_PATH = "/home/vdhussa/temp/vbucketmigrator/vbucketmigrator"
    INIT, START, CHECK_TAP, TAP_REGISTER, RUN, MONITOR, ERROR, RESTART, FAIL, STOP = range(10)
    RETRY_COUNT = 3
    INIT_TIMER = 2
    MONITOR_TIMER = 10
    VBM_TIMER = 5 
    VBM_STATS_SOCK = "/var/tmp/vbs/vbm.sock"
    MAX_MONITOR_INTERVAL = 30

    def __init__(self, key, migration_mgr, mb_mgr, as_mgr):
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
        self.config = None
        self.vb_stats = None
        self.vb_dest_stats = None
        self.monitor_ts = time.time()
        asyncon.AsynConDispatcher.__init__(self, None, self.timer, self.as_mgr)
        self.create_timer()
        self.set_timer()

    def set_timer(self):
        self.timer_event.add(self.timer)

    def start(self):
        self.config = self.migration_mgr.get(self.key)
        source = self.config.get('source')
        dest = self.config.get('destination')
        vblist = self.config.get('vblist')

        if self.source_vb_set and self.dest_vb_set:
            self.state = Migrator.CHECK_TAP
            return
        if not self.source_vb_set:
            t = threading.Thread(target=self.init_vbucket, args=(source, vblist, "active"))
            t.daemon = True
            t.start()
        elif not self.dest_vb_set:
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
        host,port = hostport.split(":")
        mc = mc_bin_client.MemcachedClient(host, int(port))
        print "setting %s for %s" %(hostport, state)
        try:
            for vb in vblist:
                op, cas, data = self.setvb(mc, str(vb), state)
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

    def check_tap_registered(self):
        source = self.config.get('source')
        vblist = self.config.get('vblist')
        host,port = source.split(":")
        
        mc = mc_bin_client.MemcachedClient(host, int(port))
        try:
            response = mc.stats(Migrator.CHECKPOINT_STATS_STR)
            #Sample vb_0:cursor_checkpoint_id:eq_tapq:repli--47FD90C2
            tap_name = "repli-" + ("%X" % zlib.crc32(self.key))
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
        tapname = "repli-" + ("%X" % zlib.crc32(self.key))
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
        tapname = "repli-" + ("%X" % zlib.crc32(self.key))

        vblist_str = ",".join(str(vb) for vb in vblist)
        # Starting vBucketMigrator
        if (interface == ''):
            print "Starting without interface"
            self.vbmp = subprocess.Popen(["sudo", Migrator.VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", "-v", "-r"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        else:
            self.vbmp = subprocess.Popen(["sudo", Migrator.VBUCKET_MIGRATOR_PATH, "-h", source, "-b", vblist_str, "-d", dest, "-N", tapname, "-A", "-i", interface, "-v", "-r"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        Log.debug('Started VBM with pid %d interface %s', self.vbmp.pid, interface)
        print('Started VBM with pid %d interface %s', self.vbmp.pid, interface)
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

    #def fetch_vb_items(self):
    #    source = self.config.get('source')
    #    dest = self.config.get('destination')
    #    t = threading.Thread(target=self.get_vb_items, args=(source,True))
    #    t.daemon = True
    #    t.start()
    #    if dest != '':
    #        td = threading.Thread(target=self.get_vb_items, args=(dest,False))
    #        td.daemon = True
    #        td.start()
        

    #def get_vb_items(self, addr, source = True):
    #    dhost, dport = addr.split(":")
    #    mc = mc_bin_client.MemcachedClient(dhost, int(dport))
    #    try:
    #        stats = mc.stats(Migrator.VBUCKET_STATS_STR)
    #        if source:
    #            self.vb_stats = stats
    #        else:
    #            self.vb_dest_stats = stats
    #    except Exception, e:
    #        Log.error("Unable to get vbucket stats for %s" %addr)
    #    finally:
    #        mc.close()

    def handle_fail(self):
        Log.debug("Handle migrator fail")

    def handle_change_config(self):
        self.config = self.migration_mgr.get(self.key)
        self.restart_vbm(Migrator.INIT)

    def set_change_config(self):
        self.state = Migrator.CHANGE_CONFIG

    def monitor(self):
        Log.debug("Monitor vbm")
        source = self.config.get('source')
        dest = self.config.get('destination')

        if self.vbm_monitor is None:
            (host, port) = dest.split(':')
            vbm_stats_sock = Migrator.VBM_STATS_SOCK + "." + host
            print "Sock: %s" %vbm_stats_sock
            params={"addr":vbm_stats_sock, "name":dest, "timeout":Migrator.VBM_TIMER, "mgr":self.as_mgr}
            self.vbm_monitor = mbMigratorHandler. MBMigratorHandler(params)
        else:
            stats = self.vbm_monitor.get_stats()
            if not stats is None and self.is_vbm_progressing(stats):
                Log.debug("VBM progress is fine")
            elif not stats:
                Log.error("VBM %s is not running. Will try restarting" %dest)
                self.state = Migrator.RESTART
            else:
                Log.info("VBM %s is not progressing" %dest)
        self.timer = Migrator.MONITOR_TIMER
        self.monitor_ts = time.time()

    def is_alive(self):
        cur_time = time.time()
        if cur_time - self.monitor_ts > Monitor.MAX_MONITOR_INTERVAL:
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
        os.kill(self.vbmp.pid, signal.SIGTERM)
        self.state = state
        self.timer = Migrator.INIT_TIMER

    def kill_migrator(self):
        source = self.config.get('source')
        dest = self.config.get('destination')
        self.set_vbucket_state(source, vblist, "dead")
        self.set_vbucket_state(dest, vblist, "dead")
        # Stop - kill the VBM and return
        Log.info('Stop request for key %s, will kill the vbucket migrator (pid %d)', self.key, self.vbmp.pid)
        os.kill(self.vbmp.pid, signal.SIGTERM)

    def stop_migrator(self):
        self.state = Migrator.STOP

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
        elif self.state == Migrator.CHANGE_CONFIG:
            # Handle config change
            self.change_config()

    def handle_timer(self):
        print "Handle timer! %d" %self.state
        self.handle_states()
        self.set_timer()

