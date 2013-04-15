#!/usr/bin/env python
import migrator
import asyncon
import threading
import utils
import json

from logger import *

Log = getLogger()

class MigrationManager(asyncon.AsynConDispatcher):
    """Class to parse the config and manage the migrators (which run the VBMs)"""
    INIT, CONFIG, MONITOR, STOP, END = range(5)
    DEFAULT_MB_PORT = 11211

    def __init__(self, vbs_manager):
        self.rowid = 0
        self.vbtable = {}
        self.transfer_vbtable = {}
        self.vbs_manager = vbs_manager
        self.state = MigrationManager.INIT
        self.as_mgr = asyncon.AsynCon()
        self.timer = 5
        self.send_config_response = False
        self.ifaces = utils.get_all_interfaces()
        self.err_queue = []
        asyncon.AsynConDispatcher.__init__(self, None, self.timer, self.as_mgr)
        self.create_timer()
        self.set_timer()

    def create_migrator(self, key, row):
        migrator_obj = migrator.Migrator(key, self, None, self.as_mgr)
        row['migrator'] = migrator_obj
        return migrator_obj

    def end_migrator(self, key):
        migrator_obj = self.vbtable[key].get('migrator')
        migrator_obj.stop_migrator()

    def set_config(self, config):
        self.config = config
        self.state = MigrationManager.CONFIG

    def parse_config_row(self, row):
        source = row.get('Source')
        if (':' not in source):
            source = source + ':' + str(MigrationManager.DEFAULT_MB_PORT)

        dest = row.get('Destination')
        if (dest != '' and ':' not in dest):
            dest = dest + ':' + str(MigrationManager.DEFAULT_MB_PORT)

        vblist = row.get('VbId')

        if (source == '' or len(vblist) == 0):
            raise RuntimeError("For row [" + str(row) + "], source/vbucket list missing")
            
        vblist.sort()
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
        new_vb_table = {}
        heartbeat_interval = 10
        if ('HeartBeatTime' in self.config):
            heartbeat_interval = self.config['HeartBeatTime']
            self.timer = heartbeat_interval

        config_data = self.config.get('Data')
        if (config_data == None or len(config_data) == 0):
            Log.warning('VBucket map missing in config')
            self.vbs_manager.send_error(json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":["No Vbucket map in config"]}))
            return
            #return json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":["No Vbucket map in config"]})

        Log.debug('New config from VBS: %s', str(config_data))

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
        for row in config_data:
            try:
                (key, value) = self.parse_config_row(row)
                (ip,port) = value['source'].split(':')
                value['interface'] = self.get_iface_for_ip(ip)
                new_vb_table[key] = value
                #Check for transfer and create a new row if needed
                if value.has_key('Transfer_VbId') and (value['Transfer_VbId'] is not None) and len(value['Transfer_VbId']) > 0:
                    key_transfer = key+"_transfer"
                    value['transfer'] = True
                    value['vblist'] = value['Transfer_VbId']
                    new_vb_table[key_transfer] = value
            except RuntimeError, (message):
                err_details.append(message)

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
            for (k, v) in new_vb_table.iteritems():
                if k in self.vbtable and self.vbtable[k].get('migrator').is_alive():
                    #Copy the existing migrator and reset state
                    v['migrator'] =  self.vbtable[k].get('migrator')
                    v['migrator'].set_change_config()
                else:
                    # Start a new VBM
                    Log.debug('Starting a new vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
                    new_migrators.append(self.create_migrator(k,v))

                # Iterate over the old table and:
                #   If the key is not found in the new table, kill the VBM for that row
                for (k, v) in self.vbtable.iteritems():
                    if k not in new_vb_table:
                        # Kill the VBM for the row
                        Log.debug('Killing vbucket migrator for row [%s] with vbucket list %s', k, v['vblist'])
                        self.end_migrator(k)

        self.vbtable = new_vb_table

        Log.info("New table after parsing config: ")
        for (k, v) in new_vb_table.iteritems():
            Log.info(str(v))

        self.state = MigrationManager.MONITOR

        if (len(err_details) > 0):
            self.vbs_manager.send_error(json.dumps({"Cmd":"Config", "Status":"ERROR", "Detail":err_details}))
            return False
        else:
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
        Log.debug("If send_config_response is set, get checkpoints from backup daemon")
        if self.send_config_response:
            if self.config_response is not None:
                self.vbs_manager.send_message(self.config_response)
                self.send_config_response = False
            else:
                t = threading.Thread(target=self.get_checkpoints)
                t.daemon = True
                t.start

    def get_checkpoints(self):
        cp_vb_ids = self.config.get("RestoreCheckPoints")
        #TODO: Get the restore checkpoints from backup daemon
        cp_arr = []
        for i in range(len(cp_vb_ids)):
            cp_arr.append(0)

        self.config_response = {"Cmd":"Config", "Status":"OK", "Vbuckets":{"Replica":cp_vb_ids}, "CheckPoints":{"Replica":cp_arr}}
        
    def handle_timer(self):
        print "Handle timer! %d" %self.state
        self.handle_states()
        self.set_timer()

    def handle_states(self):
        if self.state == MigrationManager.INIT:
            Log.debug("Init state... waiting")
        elif self.state == MigrationManager.CONFIG:
            print "Handle config"
            self.handle_new_config()
        elif self.state == MigrationManager.MONITOR:
            self.monitor()
        elif self.state == MigrationManager.STOP:
            self.stop_migrators()
        elif self.state == MigrationManager.END:
            Log.debug("Nothing to do!")

