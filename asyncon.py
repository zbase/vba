import errno
import libevent
import socket
import os

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, ECONNRESET, \
     ENOTCONN, ESHUTDOWN, EINTR, EISCONN, errorcode

"""
    Wrapper class on libevent to provide an asyncore like interface
"""
class AsynCon(object):
    EV_READ = libevent.EV_READ
    EV_WRITE = libevent.EV_WRITE
    EV_TIMEOUT = libevent.EV_TIMEOUT
    EV_TIMER = 5

    def __init__(self):
        self._base = libevent.Base()
        self._map = {}

    def loop(self):
        self._base.loop()

    def get_base(self):
        return self._base

    # For a map for conn objects that can be accessed by the app
    # @optional
    def add_connection(self, conObj):
        self._map[conObj.fileno()] = conObj


"""
    Connection wrapper class 
    handles connection creation, events etc
"""
class AsynConDispatcher(object):
    def __init__(self, sock=None, timeout=5.0, mgr=None, outIp=None):
        self.socket = None
        self.read_event = None
        self.write_event = None
        self.timeout_event = None
        self.addr = addr = None
        self.connected = False
        self._mgr = mgr
        self._base = mgr._base
        self.accepting = False
        self.closing = False
        self.timeout = 5#timeout
        self._map = mgr._map
        self.outIp = outIp

        if not sock is None:
            self.socket = sock
            self.socket.setblocking(False)
            self.connected = True
            self.manage_socket()
            self.create_events()
            try:
                self.addr = self.socket.getpeername()
            except socket.error:
                pass

    def __repr__(self):
        status = [self.__class__.__module__+"."+self.__class__.__name__]
        if self.accepting and self.addr:
            status.append('listening')
        elif self.connected:
            status.append('connected')
        if self.addr is not None:
            try:
                status.append('%s:%d' % self.addr)
            except TypeError:
                status.append(repr(self.addr))
        return '<%s at %#x>' % (' '.join(status), id(self))

    def fileno(self):
        return self._fileno

    def manage_socket(self):
        if not self.outIp is None:
            self.socket.bind((self.outIp, 0))
        self._fileno = self.socket.fileno()
        self._map[self._fileno] = self.socket

    def create_socket(self, family, type):
        self.family_and_type = family, type
        self.socket = socket.socket(family, type)
        self.socket.setblocking(False)
        self.manage_socket()

    def set_reuse_addr(self):
        # try to re-use a server port if possible
        try:
            self.socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR,
                self.socket.getsockopt(socket.SOL_SOCKET,
                                       socket.SO_REUSEADDR) | 1
                )
        except socket.error:
            pass

    # Filters for setting the socket as readable/writeable
    # Read/write events will be added on timeout
    # for compatibility (unused)
    def readable(self):
        return True

    # for compatibility (unused)
    def writable(self):
        return True

    def listen(self, num):
        self.accepting = True
        if os.name == 'nt' and num > 5:
            num = 1
        self.create_events()
        return self.socket.listen(num)

    def bind(self, addr):
        self.addr = addr
        return self.socket.bind(addr)

    def connect(self, address):
        self.connected = False
        err = self.socket.connect_ex(address)
        self.create_events()
        # XXX Should interpret Winsock return values
        if err in (EINPROGRESS, EALREADY, EWOULDBLOCK):
            return
        if err in (0, EISCONN):
            self.addr = address
            self.connected = True
            self.handle_connect()
        else:
            raise socket.error, (err, errorcode[err])

    def accept(self):
        # XXX can return either an address pair or None
        try:
            conn, addr = self.socket.accept()
            return conn, addr
        except socket.error, why:
            if why[0] == EWOULDBLOCK:
                pass
            else:
                raise

    def send(self, data):
        try:
            result = self.socket.send(data)
            return result
        except socket.error, why:
            if why[0] == EWOULDBLOCK:
                return 0
            else:
                raise
            return 0

    def recv(self, buffer_size):
        try:
            data = self.socket.recv(buffer_size)
            if not data:
                # a closed connection is indicated by signaling
                # a read condition, and having recv() return 0.
                self.handle_close()
                return ''
            else:
                return data
        except socket.error, why:
            # winsock sometimes throws ENOTCONN
            if why[0] in [ECONNRESET, ENOTCONN, ESHUTDOWN]:
                self.handle_close()
                return ''
            else:
                raise

    def close(self):
        if self.read_event is not None:
            self.read_event.delete()
        if self.write_event is not None:
            self.write_event.delete()
        if self.timeout_event is not None:
            self.timeout_event.delete()
        if self.timer_event is not None:
            self.timer_event.delete()
        if self.socket is not None:
            self.socket.close()

    def enable_read(self):
        self.read_event.add()

    def enable_write(self):
        self.write_event.add()

    def handle_read_event(self, evt, fd, what, obj):
        if self.accepting:
            # for an accepting socket, getting a read implies
            # that we are connected
            if not self.connected:
                self.connected = True
            self.handle_accept()
        elif not self.connected:
            self.handle_connect()
            self.connected = True
            self.handle_read()
        else:
            self.handle_read()

    def handle_write_event(self, evt, fd, what, obj):
        # getting a write implies that we are connected
        if not self.connected:
            self.handle_connect()
            self.connected = True
        self.handle_write()

    def enable_timeout(self):
        self.timeout_event.add(self.timeout)

    def handle_timer_event(self, _, evt):
        self.handle_timer()


    def handle_timeout_event(self, evt, fd, what, obj):
        if not self.connected:
            self.log_info('connection not established','warning')
        self.handle_timeout()

    def handle_expt_event(self):
        self.handle_expt()

    def handle_error(self):
        self.log_info('unimplemented handle error', 'error')
        #handle error

    def handle_expt(self):
        self.log_info('unhandled exception', 'warning')

    def handle_read(self):
        self.log_info('unhandled read event', 'warning')

    def handle_write(self):
        self.log_info('unhandled write event', 'warning')

    def handle_connect(self):
        self.log_info('unhandled connect event', 'warning')

    def handle_accept(self):
        self.log_info('unhandled accept event', 'warning')

    def handle_timer(self):
        self.log_info('unhandled timer event', 'warning')

    def handle_timeout(self):
        self.log_info('unhandled timeout event', 'warning')

    def handle_close(self):
        self.log_info('unhandled close event', 'warning')
        self.close()

    def create_timer(self):
        self.timer_event = libevent.Timer(self._base, self.handle_timer_event, self)

    def create_events(self):
        self.write_event = libevent.Event(self._base, self._fileno, AsynCon.EV_WRITE, self.handle_write_event, self)
        self.read_event = libevent.Event(self._base, self._fileno, AsynCon.EV_READ, self.handle_read_event, self)
        self.timeout_event = libevent.Event(self._base, self._fileno, AsynCon.EV_TIMEOUT, self.handle_timeout_event, self)
        #self.read_event.add()
        #self.timeout_event.add(self.timeout)
        #self.write_event.add()

    def set_event(self, what, freq=None):
        if what == AsynCon.EV_READ:
            self.read_event.add(freq)
        elif what == AsynCon.EV_WRITE:
            self.write_event.add(freq)
        elif what == AsynCon.EV_TIMEOUT:
            self.timeout_event.add(freq)
        elif what == Asyncon.EV_TIMER and freq is not None:
            self.timer_event.add(freq)
        else:
            self.log_info("unhandled event add request", "warning")

    # log and log_info may be overridden to provide more sophisticated
    # logging and warning methods. In general, log is for 'hit' logging
    # and 'log_info' is for informational, warning and error logging.

    def log(self, message):
        sys.stderr.write('log: %s\n' % str(message))

    def log_info(self, message, type='info'):
        if __debug__ or type != 'info':
            print '%s: %s' % (type, message)

