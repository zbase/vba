#!/usr/bin/env python26

#   Copyright 2013 Zynga Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import sys, os
import thread, threading
import pdb
import socket
import commands
import string
import json

from vbaLogger import *
from vbaConstants import *

imported = False
Log = getLogger()

try:
    from vbucket_restore import vbucketRestore
    imported = True
except:
    Log.info("Unable to import vbucketRestore from vbucket_restore of backup tools")

class vbucketRestoreWrapper:

    def __init__(self):
        if imported == True:
            self.vbucket_restore = vbucketRestore()

    def get_storage_server(self, vb_id):
        if imported == True:
            return self.vbucket_restore.get_storage_server(vb_id)
        else:
            storage_server = []
            return storage_server

    def get_checkpoints(self, vb_list):
        if imported == True:
            return self.vbucket_restore.get_checkpoints(vblist)
        checkpoint_list = {}
        for vb_id in vb_list:
            checkpoint_list[vb_id] = -1
        return True, checkpoint_list

    def restore_vbuckets(self, vb_list):
        if imported == True:
            return self.vbucket_restore.restore_vbuckets(vblist)

        output_status = []

        for vb_id in vb_list:
            restore_status = {}
            restore_status['vb_id'] = vb_id
            restore_status['status'] = "Restore Unsuccessful"

            output_status.append(restore_status)

        return output_status


if __name__ == '__main__':

    #unit test
    disk_mapper = "172.21.13.72"
    vb_list = [25, 26, 27]
    vb_restore = vbucketRestoreWrapper()
    status, checkpoint_list = vb_restore.get_checkpoints(vb_list)

    if status == True:
        print checkpoint_list
    else:
        print "Checkpoint list failed :("

    output = vb_restore.restore_vbuckets(vb_list)
    print output





