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
import socket
import time
import sys
import os
import subprocess

server_address = '/tmp/test_uds'

def get_membase_stats():
    cmd_str = "echo 'stats' | nc 0 11211 "
    statp = subprocess.Popen([cmd_str], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()[0]
    return statp

# Make sure the socket does not already exist
try:
    os.unlink(server_address)
except OSError:
    if os.path.exists(server_address):
        raise

# Create a UDS socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
# Bind the socket to the port
print >>sys.stderr, 'starting up on %s' % server_address
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)

while True:
    # Wait for a connection
    print >>sys.stderr, 'waiting for a connection'
    connection, client_address = sock.accept()
    try:
        print >>sys.stderr, 'connection from', client_address
        data = get_membase_stats()
        print "Sending %s" %data
        connection.sendall(data)
        time.sleep(2)
    except Exception, e:
        print "Error: %s" %e
    finally:
        # Clean up the connection
        connection.close()
        

