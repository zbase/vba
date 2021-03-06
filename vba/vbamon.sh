#! /bin/bash
#
# description:  vba startup script
# processname: vbamon.sh
# location: /opt/vba/vbamon.sh

# Source function library.
. /etc/rc.d/init.d/functions

USER=nobody
PIDFILE="/var/run/vbs/vba.pid"
VBUCKETMIGRATOR=/opt/zbase/bin/vbucketmigrator

# keep vbucket_agent running  continuously
while :; do
    echo "Starting VBA"
    sudo killall $VBUCKETMIGRATOR
    sudo python /opt/vba/vba.py -f /etc/sysconfig/vbs_server_ip &
    RETVAL=$?
    if [ $RETVAL -ne 0 ];then
        echo $RETVAL
        exit 0
    fi
    echo $! > $PIDFILE

    while :; do
        sleep 10
        echo "Will check for VBA"
        if [[ ! -f "$PIDFILE" ]];then   #vba has been manually stopped
            continue
        fi
        read pid < "$PIDFILE"
        echo "Read pid $pid"
        if [[ -n "$pid" && -d "/proc/$pid" ]];then
            continue
        else    # vba is not running, restart it
            break
        fi
    done
done

