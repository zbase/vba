#!/bin/sh
#
# chkconfig: - 55 45
# description:	vba - vbucket agent
# processname: vbucket_agent.py

# Source function library.
. /etc/rc.d/init.d/functions

USER=nobody
MONPIDFILE=/var/run/vbs/vbamon.pid
VBAPIDFILE=/var/run/vbs/vba.pid
MONVBA=/usr/bin/vbamon.sh
VBUCKETMIGRATOR=/opt/membase/bin/vbucketmigrator

# Check that networking is up.
if [ "$NETWORKING" = "no" ]
then
	exit 0
fi

RETVAL=0
prog="vba"


start () {
    mkdir -m 755 -p /var/run/vbs
    chown $USER /var/run/vbs
    # check if another instance of vbamon is already running 
    if [[ -f "$MONPIDFILE" ]];then
        read pid < "$MONPIDFILE"
    fi
    if [[ ! -f "$MONPIDFILE" || -z "$pid" ]];then
        pid=$(pidof $MONVBA)
    fi 
    if [[ -n "$pid" && -d "/proc/$pid" ]];then
        echo "Already running..."
        exit 0
    fi 

    # cleanup any vbucket agents, vbucketmigrators still running
    killproc -p $VBAPIDFILE vbucket_agent.py
    sudo killall $VBUCKETMIGRATOR 2> /dev/null

    $MONVBA > /dev/null 2>&1 &
    rc=$?
    pid=$!

    if [ $rc == 0 ] ; then
        cmd='/bin/true'
        echo "$pid" > "$MONPIDFILE"
    else
        cmd='/bin/false'
        rm -f $VBAPIDFILE
        rm -rf $MONPIDFILE
        touch /var/lock/subsys/vba
    fi
    action $"Starting $prog: " $cmd
}

stop () {
    echo -n $"Stopping $prog: "
    killproc $MONVBA
    killproc -p $VBAPIDFILE vbucket_agent.py
    sudo killall $VBUCKETMIGRATOR
    RETVAL=$?
    echo
    if [ $RETVAL -eq 0 ] ; then
        rm -f /var/lock/subsys/vba
        rm -f $VBAPIDFILE
        rm -f $MONPIDFILE
    fi
}

restart () {
        stop
        start
}


# See how we were called.
case "$1" in
  start)
	start
	;;
  stop)
	stop
	;;
  status)
	status -p /var/run/vbs/vba.pid vbucket_agent.py
	;;
  restart|reload)
	restart
	;;
  *)
	echo $"Usage: $0 {start|stop|status|stats|restart|reload}"
	exit 1
esac

exit $?
