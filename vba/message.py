import struct
from logger import *

Log = getLogger()

"""class for creating message"""
class VBSMessage:
    @staticmethod
    def getMsg(cmd):
        Log.info("Sending message %si len: %d" %(cmd, len(cmd)))
        msg = cmd

        tot = struct.pack("!I",len(msg))
        return tot+msg
