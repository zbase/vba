Vbucket Agent (VBA)
===================

#### How to build ?

To build a rpm of VBA, use:

    $ ./build-rpm.sh

### How to start VBA after RPM installation

Create VBA config file (/etc/sysconfig/vbs_server_ip) which contains ip of the machine, on which Vbucketserver is running.

VBA can be started using following command:

    # /etc/init.d/vba start

