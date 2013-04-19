import logging

from logging.handlers import SysLogHandler

Log = None
    
def getLogger():
    global Log

    if(Log == None):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s [%(process)d] %(levelname)-8s %(filename)s %(lineno)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S')
        
        Log = logging.getLogger('VBA')
        syslog = SysLogHandler(address='/dev/log')
        formatter = logging.Formatter('%(asctime)s [%(process)d] %(levelname)-8s %(filename)s %(lineno)s %(message)s')
        syslog.setFormatter(formatter)
        Log.addHandler(syslog)

    return Log
