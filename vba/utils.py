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
import sys
MEMBASE_LIB_DIR="/opt/membase/lib/python"
sys.path.append(MEMBASE_LIB_DIR)

import getopt
import exceptions
import socket
import struct
import fcntl
import array
import mc_bin_client
import memcacheConstants

# Util functions copied from 
# /opt/membase/lib/python/mbvbucketctl


def listvb(mc, username=None, password=""):
    if username:
        mc.sasl_auth_plain(username, password)
    vbs = mc.stats('vbucket')
    for (vb, state) in sorted(list(vbs.items())):
        print "vbucket", vb[3:], state

def setvb(mc, vbid, vbstate, username=None, password=""):
    if username:
        mc.sasl_auth_plain(username, password)
    return mc.set_vbucket_state(int(vbid), vbstate)

def rmvb(mc, vbid, username=None, password=""):
    if username:
        mc.sasl_auth_plain(username, password)
    return  mc.delete_vbucket(int(vbid))

DEFAULT_PORT = "11210"
DEFAULT_HOST_PORT = ["127.0.0.1", DEFAULT_PORT]

def parse_args(args):
    host_port = DEFAULT_HOST_PORT
    tap_name = ''
    is_registration = True
    # By default, the TAP client receives mutations from the open checkpoint as well.
    closed_checkpoint_only = 0x00
    last_closed_checkpoint_id = -1
    # By default, we disable backfill for the registered TAP client.
    enable_backfill = False

    try:
        opts, args = getopt.getopt(args, 'h:r:d:l:cb', ['help'])
    except getopt.GetoptError, e:
        usage(e.msg)

    for (o, a) in opts:
        if o == '--help':
            usage()
        elif o == '-h':
            host_port = a.split(':')
            if len(host_port) < 2:
                host_port = [a, DEFAULT_PORT]
        elif o == '-r':
            tap_name = a
        elif o == '-d':
            tap_name = a
            is_registration = False
        elif o == '-c':
            closed_checkpoint_only = 0x01
        elif o == '-b':
            enable_backfill = True # Do backfill if required.
        elif o == '-l':
            last_closed_checkpoint_id = a
        else:
            usage("unknown option - " + o)

    if len(tap_name) == 0:
        usage("missing name argument, which is the registered client name")
    return host_port, tap_name, is_registration, closed_checkpoint_only, \
           last_closed_checkpoint_id, enable_backfill

def readTap(mc):
    ext = ''
    key = ''
    val = ''
    cmd, vbucketId, opaque, cas, keylen, extlen, data = mc._recvMsg()
    if data:
        ext = data[0:extlen]
        key = data[extlen:extlen+keylen]
        val = data[extlen+keylen:]
    return cmd, opaque, cas, vbucketId, key, ext, val

def encodeVBucketList(vbl):
    l = list(vbl) # in case it's a generator
    vals = [struct.pack("!H", len(l))]
    for v in vbl:
        vals.append(struct.pack("!H", v))
    return ''.join(vals)

def encodeTAPConnectOpts(opts):
    header = 0
    val = []
    for op in sorted(opts.keys()):
        header |= op
        if op in memcacheConstants.TAP_FLAG_TYPES:
            val.append(struct.pack(memcacheConstants.TAP_FLAG_TYPES[op],
                                   opts[op]))
        elif op == memcacheConstants.TAP_FLAG_CHECKPOINT:
            total = int(opts[op][0])
            if total == 0:
                continue
            val.append(struct.pack(">H", total))
            for i in range(total):
                val.append(struct.pack(">HQ", opts[op][1][i], opts[op][2][i]))
        elif op == memcacheConstants.TAP_FLAG_LIST_VBUCKETS:
            val.append(encodeVBucketList(opts[op]))
        else:
            val.append(opts[op])
    return struct.pack(">I", header), ''.join(val)

def register_tap_name(host_port, tap_name, vblist, ckpoint_vblist, cklist):
    is_registration = True
    closed_checkpoint_only = 0x00
    last_closed_checkpoint_id = -1
    enable_backfill = True 

    try:
        mc = mc_bin_client.MemcachedClient(host_port[0], int(host_port[1]))
        backfill_age = 0x00000000
        if enable_backfill == False:
            backfill_age = 0xffffffff
        ext, val = encodeTAPConnectOpts({
        ## The three args for TAP_FLAG_CHECKPOINT represents the number of vbuckets,
        ## the list of vbucket ids, and their last closed checkpoint ids. At this time,
        ## we only support a single vbucket 0.
        memcacheConstants.TAP_FLAG_CHECKPOINT: (len(ckpoint_vblist), ckpoint_vblist, cklist),
        memcacheConstants.TAP_FLAG_SUPPORT_ACK: '',
        memcacheConstants.TAP_FLAG_REGISTERED_CLIENT: closed_checkpoint_only,
        memcacheConstants.TAP_FLAG_BACKFILL: backfill_age,
        memcacheConstants.TAP_FLAG_CKSUM: '',
        memcacheConstants.TAP_FLAG_LIST_VBUCKETS: (vblist)
        })
        mc._sendCmd(memcacheConstants.CMD_TAP_CONNECT, tap_name, val, 0, ext)
        cmd, opaque, cas, vbucketId, key, ext, val = readTap(mc)
        if cmd == memcacheConstants.CMD_TAP_OPAQUE:
            return 0
    except mc_bin_client.MemcachedError as ne:
        return 1 
    except socket.error:
        return 2
    finally:
        if mc:
           mc.close()
    return 1

def deregister_tap_name(host_port, tap_name):
    try:
        mc = mc_bin_client.MemcachedClient(host_port[0], int(host_port[1]))
        mc.deregister_tap_client(tap_name)
    except mc_bin_client.MemcachedError as ne:
        return 1 
    except socket.error:
        return 2
    finally:
        if mc:
           mc.close()
    return 0

#Diff between two lists
def diff(a, b):
    b = set(b)
    return [aa for aa in a if aa not in b]

#Return map of {ip: interface_name}
def get_all_interfaces():
    is_64bits = sys.maxsize > 2**32
    struct_size = 40 if is_64bits else 32
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    max_possible = 8 # initial value
    ret = {}
    while True:
        bytes = max_possible * struct_size
        names = array.array('B', '\0' * bytes)
        outbytes = struct.unpack('iL', fcntl.ioctl(
            s.fileno(),
            0x8912,  # SIOCGIFCONF
            struct.pack('iL', bytes, names.buffer_info()[0])
        ))[0]
        if outbytes == bytes:
            max_possible *= 2
        else:
            break
    namestr = names.tostring()
    for i in range(0, outbytes, struct_size):
        iface = namestr[i:i+16].split('\0', 1)[0]
        if iface != 'lo':
            ret[socket.inet_ntoa(namestr[i+20:i+24])] = iface
    return ret
