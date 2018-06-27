#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2013 Red Hat, Inc.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#

"""
Utility functions
"""

import atexit
import errno
import glob
import logging
import os
import socket
import threading
import time
import mmap
import weakref
from contextlib import contextmanager, closing
from six.moves import queue

from yajsonrpc import stomp

from ovirt_hosted_engine_ha.env import constants as envconst
from .exceptions import DisconnectionError
from . import monotonic
from . import engine

from vdsm.config import config as vdsmconfig
from vdsm import client
from vdsm.virt import vmstatus

_vdsm_json_rpc = None
_vdsm_json_rpc_lock = threading.Lock()

# TODO we need to make this configurable
VDSM_MAX_RETRY = 60
VDSM_DELAY = 1


def has_elapsed(start, count, end=None):
    """
    Returns true if 'count' seconds have elapsed between timestamps 'start'
    and 'end'.  If 'end' is not specified, defaults to time.time().  A
    starting time of None results in False.
    """
    if start is None:
        return False
    if end is None:
        end = time.time()
    return (end - start >= count)


def mkdir_recursive(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def socket_readline(sock, log,
                    isTimed=False,
                    timeoutSec=envconst.MESSAGE_READ_TIMEOUT_SEC):
    """
    Reads a line from socket.
    Returns string read (without trailing newline),
    Raises either DisconnectionError on disconnect or timeout (default 30 sec).
    """
    prevTimeout = sock.gettimeout()
    try:
        if not isTimed:
            # No timeout
            log.debug('socket_readline in blocking mode')
            sock.setblocking(1)
            sockfile = sock.makefile()
            msg = sockfile.readline()
        else:
            # Reading a line with timeout
            log.debug(
                'socket_readline with {timeout} seconds timeout'.format(
                    timeout=timeoutSec,
                )
            )
            msg = ""
            rcvChar = 0
            sock.settimeout(float(timeoutSec))
            while rcvChar != '\n':
                rcvChar = sock.recv(1)
                msg = msg + rcvChar

    except socket.timeout:
        log.debug("Connection timeout while reading from socket")
        raise DisconnectionError("Connection timed out")

    except IOError as e:
        log.debug("Connection closed while reading from socket: %s", str(e))
        raise DisconnectionError("Connection closed")

    finally:
        sock.settimeout(prevTimeout)

    if len(msg) == 0:
        log.debug("Connection closed while reading from socket")
        raise DisconnectionError("Connection closed")
    else:
        msg = msg.strip()
        return msg


def socket_sendline(sock, log, data):
    """
    Writes data to a socket, appending a newline.  Returns normally, or
    raises DisconnectionError if the write could not be completed.
    """
    try:
        sock.sendall(data + "\n")
    except IOError as e:
        log.debug("Connection closed while writing to socket: %s", str(e))
        raise DisconnectionError("Connection closed")
    return


def to_bool(string):
    first = str(string).lower()[:1]
    if first in ('t', 'y', '1'):
        return True
    elif first in ('f', 'n', '0'):
        return False
    else:
        raise ValueError("Invalid value for boolean: {0}".format(string))


def engine_status_score(status):
    """
    Convert a dict engine/vm status to a sortable numeric score;
    the highest score is a live vm with a healthy engine.
    """
    if status['vm'] == 'unknown':
        return 0

    if status['vm'] in (
        engine.VMState.DOWN,
        engine.VMState.DOWN_UNEXPECTED,
        engine.VMState.DOWN_MISSING
    ):
        return 1

    if (status['vm'] == engine.VMState.UP and
            status['detail'] == vmstatus.PAUSED):
        return 2

    if status['health'] == engine.Health.BAD:
        return 3

    if status['health'] == engine.Health.GOOD:
        return 4

    raise ValueError("Invalid engine status: %r" % status)


@contextmanager
def aligned_buffer(size):
    """Context manager that creates a file like object in shared memory.
       MMAPped memory is always page aligned and this can be used to
       work with direct IO files.
    """
    buf = mmap.mmap(-1, size, mmap.MAP_PRIVATE)
    with closing(buf):
        yield buf


def uninterruptible(method, *args, **kwargs):
    """Make sure to repeat an operation when EINTR is received."""
    while True:
        try:
            return method(*args, **kwargs)
        except OSError as e:
            if e.errno != errno.EINTR:
                raise


def isOvirtNode():
    return (os.path.exists('/etc/rhev-hypervisor-release') or
            bool(glob.glob('/etc/ovirt-node-*-release')))


class ConnectionClosed(Exception):
    pass


class _EventBroadcaster(object):
    RECONNECT_WAIT_TIME_SECONDS = 20
    CONNECTION_CHECK_INTERVAL_SEC = 20
    EVENT_QUEUE_TIMEOUT = 1

    def __init__(self):
        self._log = logging.getLogger("%s._EventDistributor" % __name__)
        self._event_queue_name = vdsmconfig.get("addresses", "event_queue")

        self._queues = weakref.WeakSet()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

        self._vdsm_client_queue = None
        self._sub_id = None
        self._connection_ok = False
        self._last_check_time = monotonic.time()

        self._thread_running = True
        self._thread = threading.Thread(target=self._thread_func)
        self._thread.daemon = True
        self._thread.start()

    def register_queue(self, queue):
        """
        Registers queue to receive events from vdsm.

        Only a weak_ref to the queue is kept, so explicit
        unregistering is not needed.

        :param queue: Queue instance to receive events
        """

        if not self._thread_running:
            raise RuntimeError(
                "Cannot register queue, broadcaster thread is not running."
            )

        with self._lock:
            self._queues.add(queue)
            # Signal the thread that a queue was added
            self._condition.notify_all()

    def notify_connection_was_closed(self):
        self._connection_ok = False

    def close(self):
        if not self._thread_running:
            return

        self._thread_running = False
        with self._lock:
            self._condition.notify()

        self._thread.join()
        self._queues.clear()

    def _thread_func(self):
        while self._thread_running:
            try:
                self._unsubscribe_and_wait()

                if not self._thread_running:
                    break

                if not self._is_subscribed():
                    self._subscribe()

                self._handle_event()
                self._check_connection()

            except ConnectionClosed:
                self._sub_id = None
                self._vdsm_client_queue = None

            except Exception as e:
                # Error; Wait for a while before trying again
                self._log.error(e)
                time.sleep(self.RECONNECT_WAIT_TIME_SECONDS)

        if self._is_subscribed():
            self._unsubscribe()

    def _subscribe(self):
        cli = connect_vdsm_json_rpc(self._log)
        self._connection_ok = True
        self._update_connection_check_time()

        self._vdsm_client_queue = queue.Queue()
        self._sub_id = cli.subscribe(
            self._event_queue_name,
            self._vdsm_client_queue
        )

    def _unsubscribe(self):
        cli = get_vdsm_json_rpc_wo_reconnect(self._log)
        self._update_connection_check_time()
        if cli is not None:
            cli.unsubscribe(self._sub_id)

        self._sub_id = None
        self._vdsm_client_queue = None

    def _is_subscribed(self):
        return self._sub_id is not None

    def _unsubscribe_and_wait(self):
        with self._lock:
            if len(self._queues) != 0:
                return

            if self._is_subscribed():
                self._unsubscribe()

            # Wait until a queue registered or thread is exiting
            while len(self._queues) == 0 and self._thread_running:
                self._condition.wait()

    def _handle_event(self):
        try:
            event = self._vdsm_client_queue.get(
                timeout=self.EVENT_QUEUE_TIMEOUT
            )
        except queue.Empty:
            # Return, to check if connection is active or thread is exiting
            return

        self._update_connection_check_time()

        if event is None:
            raise ConnectionClosed()

        with self._lock:
            queues = list(self._queues)

        for q in queues:
            q.put(event)

    def _update_connection_check_time(self):
        self._last_check_time = monotonic.time()

    def _check_connection(self):
        check_time = (self._last_check_time +
                      self.CONNECTION_CHECK_INTERVAL_SEC)

        if monotonic.time() >= check_time:
            self._log.debug(
                "No events received for a while, checking connection."
            )
            self._update_connection_check_time()
            cli = get_vdsm_json_rpc_wo_reconnect(self._log)
            if cli is None:
                raise ConnectionClosed()

        if not self._connection_ok:
            raise ConnectionClosed()


__event_broadcaster = None


def event_broadcaster():
    global __event_broadcaster

    if __event_broadcaster is None:
        __event_broadcaster = _EventBroadcaster()

        # Close broadcaster when exiting, stop the thread and wait for it.
        # Becasue the thread is daemon, the interpreter does not wait for
        # it to finish and calls registered atexit functions
        atexit.register(__event_broadcaster.close)

    return __event_broadcaster


def __log_debug(logger, *args, **kwargs):
    if logger:
        logger.debug(*args, **kwargs)


def __vdsm_json_rpc_connect(logger=None,
                            timeout=envconst.VDSCLI_SSL_TIMEOUT):
    global _vdsm_json_rpc

    retry = 0
    while retry < VDSM_MAX_RETRY:
        try:
            _vdsm_json_rpc = client.connect(host="localhost",
                                            timeout=timeout)
            # we still have to validate the connection, also if fresh,
            # because the auto re-connect logic will not work
            # when vdsm certs got renewed at setup time by
            # host-deploy
            #
            # make sure we do not multiply the timeout by waiting
            # both here and in the check method
            retry += __vdsm_json_rpc_check(logger, VDSM_MAX_RETRY - retry)
            if _vdsm_json_rpc is not None:
                break
        except client.ConnectionError:
            retry += 1
            __log_debug(logger, 'Waiting for VDSM to connect')

        time.sleep(VDSM_DELAY)

    if _vdsm_json_rpc is None:
        raise RuntimeError(
            "Couldn't  connect to VDSM within {timeout} seconds".format(
                timeout=VDSM_MAX_RETRY * VDSM_DELAY
            )
        )


def __vdsm_json_rpc_check(logger=None, timeout=VDSM_MAX_RETRY):
    global _vdsm_json_rpc

    if _vdsm_json_rpc is None:
        return

    retry = 0
    while retry < timeout:
        retry += 1
        try:
            if _vdsm_json_rpc.Host.ping2(_timeout=5):
                # Successful ping, VDSM up and ready
                return retry

        except client.ServerError as err:
            # Wait until VDSM recovers (code 99)
            if err.code != 99:
                __log_debug(logger, 'VDSM jsonrpc connection is not ready')
                break

        except client.Error:
            __log_debug(logger, 'VDSM jsonrpc connection is not ready')
            break

        except stomp.Disconnected:
            __log_debug(logger, 'VDSM has been disconnected')
            break

        time.sleep(VDSM_DELAY)

    # VDSM is not responding, setting client to None
    event_broadcaster().notify_connection_was_closed()
    _vdsm_json_rpc = None
    return retry


def connect_vdsm_json_rpc(logger=None,
                          timeout=envconst.VDSCLI_SSL_TIMEOUT):
    """
    Get a jsonrpc connection. This method can return an existing
    connection, but validates it first.
    A reconnect attempt is made if the connection is not valid.
    """

    global _vdsm_json_rpc
    # Currently vdsm.client doesn't implement any keep-alive or
    # reconnection mechanism so the connection status has to be checked each
    # time. This could be removed once rhbz#1376843 get fixed.
    with _vdsm_json_rpc_lock:
        __vdsm_json_rpc_check(logger)
        if _vdsm_json_rpc is None:
            __log_debug(logger, 'Creating a new json-rpc connection to VDSM')
            __vdsm_json_rpc_connect(logger, timeout)

        return _vdsm_json_rpc


def get_vdsm_json_rpc_wo_reconnect(logger=None):
    """
    Get the current jsonrpc connection or None if the connection
    is not valid. This method does not try to reconnect.
    """
    global _vdsm_json_rpc

    # Check if VDSM connection is ready
    with _vdsm_json_rpc_lock:
        __vdsm_json_rpc_check(logger)
        return _vdsm_json_rpc
