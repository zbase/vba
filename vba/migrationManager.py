"""
Copyright 2013 Zynga Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
#!/usr/bin/env python
import migrator
import asyncon
import threading
import utils
import json
import copy
import mc_bin_client
import time
import sys

from Queue import *
from vbaLogger import *
from vbaConstants import *
sys.path.insert(0, BACKUP_PATH) 
from vbucket_restore import vbucketRestore 

Log = getLogger()

class MigrationManager(asyncon.AsynConDispatcher):
    """Class to parse the config and manage the migrators (which run the VBMs)"""
    INIT, CONFIG, MONITOR, STOP, END = range(5)
    DEFAULT_MB_PORT = 11211

    def __init__(self, vbs_manager, vbs_pipe_r, vbs_pipe_w):
        self.rowid = 0
        self.vbtable = {}
        self.transfer_vbtable = {}
        self.vbs_manager = vbs_manager
        self.state = MigrationManager.INIT
        self.as_mgr = asyncon.AsynCon()
        self.timer = 5
        self.send_config_response = False
        self.config_response = Queue()
        self.ifaces = utils.get_all_interfaces()
        self.err_queue = []
        asyncon.AsynConDispatcher.__init__(self, vbs_pipe_r, self.timer, self.as_mgr)
        self.buffer_size = 10
        self.create_timer()
        self.set_timer()
        self.restore = vbucketRestore()
        self.enable_read()
        self.config = Queue()
        self.recentConfig = None
        self.vbs_pipe_w = vbs_pipe_w
        self.restore_vbuckets = {}

    def create_migrator(self, key, row):
        migrator_obj = migrator.Migrator(key, self, row, None, self.as_mgr)
        row['migrator'] = migrator_obj
        return migrator_obj

    def handle_read(self):
        self.recv(self.buffer_size)
        #self.handle_states()
        self.handle_timer()
        self.enable_read()

    def end_migrator(self, key, deregister=True, vblist=None):
        migrator_obj = self.vbtable[key].get('migrator')
        migrator_obj.kill_migrator(deregister, vblist)

    def set_config(self, config):
        self.config.put(config)
        self.state = MigrationManager.CONFIG

    def parse_config_row(self, row):
        source = row.get('Source')
        if (':' not in source):
            source = source + ':' + str(MigrationManager.DEFAULT_MB_PORT)

        dest = row.get('Destination')
        if (dest != '' and ':' not in dest):
            dest = dest + ':' + str(MigrationManager.DEFAULT_MB_PORT)

        vblist = row.get('VbId')
        tvbid = row.get("Transfer_VbId")

        if (source == '' or ((vblist == None or len(vblist) == 0) and (tvbid == None or len(tvbid) == 0))):
            raise RuntimeError("For row [" + str(row) + "], source/vbucket list missing")
           
        if vblist:  
            vblist.sort()
        else:
            vblist = []
        key = source + "|" + dest
        value = {}
        value['source'] = source
        value['destination'] = dest
        value['vblist'] = vblist
        value['CheckPoints'] = row.get("CheckPoints")
        value['Transfer_VbId'] = row.get("Transfer_VbId")
        value['RestoreCheckPoints'] = row.get('RestoreCheckPoints')
        return key, value

    def get_iface_for_ip(self, src_ip):
        ret = ''
        if self.ifaces.has_key(src_ip):
            ret = self.ifaces[src_ip]

        return ret

    def handle_new_config(self):
        Log.info("inside handle_new_config")
        new_vb_table = {}
        heartbeat_interval = 10
        try:
            config = self.config.get()
            self.recentConfig = config
        except:
            return
        if ('HeartBeatTime' in config):
            heartbeat_interval = config['HeartBeatTime']
            self.timer = heartbeat_interval
        config_data = config.get('Data')
        config_data = config.get('Data')
        checkpoints = config.get('RestoreCheckPoints')

        """
        if ((config_data == None or len(config_data) == 0) and (checkpoints == None or len(checkpoints) == 0)):
            Log.warning('VBucket map missing in config')
            self.vbs_manager.send_error(json.dumps({"Cmd":"CONFIG", "Status":"ERROR", "Detail":["No Vbucket map in config"]}))
            return
        """

        Log.info('New config from VBS: %s', str(config_data))

        # Create a new table(new_vb_table) from the config data
        # The config data is of the form:
        # [
        #   {"Source":"192.168.1.1:11211", "VbId":[1,2,3], "Destination":"192.168.1.2:11211"},  
        #   {"Source":"192.168.1.1:11211", "VbId":[7,8,9], "Destination":"192.168.1.5:11511"},  
        #   .
        #   .
        # ]

        # The table we maintain is of the form:
        #   source                destination           vblist      interface      migrator
        #   192.168.1.1:11211     192.168.1.2:11211     1,2,3       eth1          
        #   192.168.1.1:11211     192.168.1.5:11511     7,8,9       eth1          
        #   .
        #   .

        err_details = []
        if config_data is not None:
            for row in config_data:
                try:
                    dest = row.get('Destination')
                    (key, value) = self.parse_config_row(row)
                    (ip,port) = value['source'].split(':')
                    value['interface'] = self.get_iface_for_ip(ip)
                    new_vb_table[key] = value
                    #Check for transfer and create a new row if needed
                    if value.has_key('Transfer_VbId') and (value['Transfer_VbId'] is not None) and len(value['Transfer_VbId']) > 0:
                        key_transfer = key+"_transfer"
                        newvalue = copy.deepcopy(value)
                        newvalue['transfer'] = True
                        newvalue['vblist'] = value['Transfer_VbId']
                        new_vb_table[key_transfer] = newvalue
                        Log.info("transfer is true. final vblist is %s", str(value['vblist'])) 
                        if value['vblist'] == None or len(value['vblist']) == 0:
                            del new_vb_table[key] 
                except RuntimeError, (message):
                    err_details.append(message)

        Log.info("here")
        new_migrators = []
        # Compare old and new vb tables
        if (len(self.vbtable) == 0):    # First time, start VBMs for all rows in new_vb_table
            for (k, v) in new_vb_table.iteritems():
                new_migrators.append(self.create_migrator(k,v))
        else:
            # Iterate over the new table and:
            #   If the row is present in the old table 
            #       reset migrator state
            #   else 
            #       start up a new VBM

            new_vbuckets = {}
            dead_keys = []

            for (k, v) in self.vbtable.iteritems():
                if k not in new_vb_table:
                    # Kill the VBM for the row
                    Log.debug('Killing vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
                    self.end_migrator(k)
                    dead_keys.append(k)

            for (k, v) in new_vb_table.iteritems():
                for vb in v['vblist']:
                    new_vbuckets[vb] = v['destination']
                Log.info("new table data %s", str(v))
                if k in self.vbtable and self.vbtable[k].get('migrator') is not None and self.vbtable[k].get('migrator').is_alive():
                    if (self.vbtable[k]['vblist'] != v['vblist']):
                    #Copy the existing migrator and reset state
                    #v['migrator'] =  self.vbtable[k].get('migrator')
                    #v['migrator'].set_change_config(v)
                        Log.debug('Vbucket list changed for row [%s] from %s to %s, will restart the vbucket migrator', k, v['vblist'], self.vbtable[k]['vblist'])
                        diff = list(set(self.vbtable[k]['vblist']) - set(v['vblist']))
                        self.end_migrator(k, False, diff)
                        dead_keys.append(k)
                        new_migrators.append(self.create_migrator(k,v))
                    else:
                        v['migrator'] = self.vbtable[k]['migrator']
                else:
                    if k in self.vbtable and self.vbtable[k].get('migrator') is None:
                        Log.info("key %s is null %s",str(k), str(self.vbtable))
                    # Start a new VBM
                    Log.debug('Starting a new vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
                    new_migrators.append(self.create_migrator(k,v))

                # Iterate over the old table and:
                #   If the key is not found in the new table, kill the VBM for that row
         
            for en in dead_keys:
                entry = self.vbtable[en]
                for vb in entry['vblist']:
                    if entry['destination'] != '':
                        if vb in new_vbuckets:
                            if entry['destination'] != new_vbuckets[vb]:
                                self.set_vbucket_state(entry['destination'], [vb], "dead") 
                                Log.info("setting remote vbucket dead %d", vb)
                        else:
                            self.set_local_vbucket_state([vb], "dead") 
                            Log.info("setting local vbucket dead %d", vb)


        self.vbtable = new_vb_table

        Log.info("New table after parsing config: ")
        for (k, v) in new_vb_table.iteritems():
            Log.info(str(v))

        Log.info("is it here")
        self.state = MigrationManager.MONITOR

        if (len(err_details) > 0):
            Log.info("inside err detail %s", str(err_details))
            self.vbs_manager.send_error(json.dumps({"Cmd":"CONFIG", "Status":"ERROR", "Detail":err_details}))
            return False
        else:
            Log.info("test")
            self.send_config_response = True
            return True

    def get(self, key):
        if self.vbtable.has_key(key):
            return self.vbtable[key]
        return None

    def run(self):
        t = threading.Thread(target=self.start_migrator_loop)
        t.daemon = True
        t.start()

    def start_migrator_loop(self):
        self.set_timer()
        self.as_mgr.loop()

    def stop_migrators(self):
        for (k, v) in self.vbtable.iteritems():
            # Kill the VBM for the row
            Log.debug('Killing vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
            self.end_migrator(k)
        self.state = MigrationManager.END

    def set_timer(self):
        self.timer_event.add(self.timer)

    def monitor(self):
        Log.info("If send_config_response is set, get checkpoints from backup daemon")
        if self.send_config_response:
            Log.info("contacting backup daemon")
            #TODO:: Remove this when restore daemon is available
            t = threading.Thread(target=self.get_checkpoints)
            t.daemon = True
            t.start()
            self.send_config_response = False
        while self.config_response.empty() is False:
            self.vbs_manager.send_message(json.dumps(self.config_response.get()))

    def setvb(self, mc, vbid, vbstate, username=None, password=""):
        if not username is None:
            mc.sasl_auth_plain(username, password)
        return mc.set_vbucket_state(int(vbid), vbstate)

    def set_vbucket_state(self, hostport, vblist, state):
        try:
            host,port = hostport.split(":")
            mc = mc_bin_client.MemcachedClient(host, int(port))
            Log.debug("setting %s for %s" %(hostport, state))
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

    def set_local_vbucket_state(self, vblist, state):
        return self.set_vbucket_state("127.0.0.1:11211", vblist, state)

    #activate the vbuckets also
    def get_checkpoints(self):
        cp_vb_ids = self.recentConfig.get("RestoreCheckPoints")
        if cp_vb_ids is None:
            return
        for id in cp_vb_ids[:]:
            if self.restore_vbuckets.get(id) == None:
                self.restore_vbuckets[id] = 1
            else:
                cp_vb_ids.remove(id)

        self.set_local_vbucket_state(cp_vb_ids, "replica")
        cp_arr = []
        Log.info("Restoring vbuckets %s" % cp_vb_ids)
        for i in range(len(cp_vb_ids)):
            cp_arr.append(0)
        ret, cp_arr_map = self.restore.get_checkpoints(cp_vb_ids)
        if ret:
            restore_status = self.restore.restore_vbuckets(cp_vb_ids)
            for i in range(len(cp_vb_ids)):
                statusMap = restore_status[i]
                ckpoint = cp_arr_map.get(cp_vb_ids[i])
                if statusMap['status'] == "Restore successful" and ckpoint != None and int(ckpoint) != -1:
                    cp_arr[i] = int(ckpoint)
                    
        Log.info("Restore checkpoint map %s %s" % (cp_vb_ids, cp_arr))
        for id in cp_vb_ids:
            try:    
                del self.restore_vbuckets[id]
            except Exception, e:
                Log.error("Could not find vBuckets for restore %s" %e)

        data = {"Cmd":"CONFIG", "Status":"OK", "Vbuckets":{"Replica":cp_vb_ids}, "CheckPoints":{"Replica":cp_arr}}
        self.config_response.put(data) 
        self.vbs_pipe_w.send("a")

    def handle_timer(self):
        Log.info("Handle timer! %d" %self.state)
        self.handle_states()
        self.set_timer()

    def handle_states(self):
        Log.info("state is %d", self.state)
        if self.state == MigrationManager.INIT:
            Log.debug("Init state... waiting")
        elif self.state == MigrationManager.CONFIG:
            print "Handle config"
            while self.config.empty() == False:
                self.handle_new_config()
        elif self.state == MigrationManager.MONITOR:
            self.monitor()
        elif self.state == MigrationManager.STOP:
            self.stop_migrators()
        elif self.state == MigrationManager.END:
            Log.debug("Nothing to do!")


