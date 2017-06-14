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

import errno
import glob
import os
import socket
import threading
import time
import mmap
from contextlib import contextmanager, closing

from yajsonrpc import stomp

from ovirt_hosted_engine_ha.env import constants as envconst
from .exceptions import DisconnectionError

from vdsm import client

_vdsm_json_rpc = None
_vdsm_json_rpc_lock = threading.Lock()

VDSM_MAX_RETRY = 15
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
    elif status['vm'] in ('down', 'already_locked'):
        return 1
    elif status['health'] == 'bad':
        return 2
    elif status['health'] == 'good':
        return 3
    else:
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


def __log_debug(logger, *args, **kwargs):
    if logger:
        logger.debug(*args, **kwargs)


def __vdsm_json_rpc_connect(logger=None,
                            timeout=envconst.VDSCLI_SSL_TIMEOUT):
    global _vdsm_json_rpc

    retry = 0
    while retry < VDSM_MAX_RETRY:
        retry += 1
        try:
            _vdsm_json_rpc = client.connect(host="localhost",
                                            timeout=timeout)
            break
        except client.ConnectionError:
            __log_debug(logger, 'Waiting for VDSM to connect')

        time.sleep(VDSM_DELAY)

    __vdsm_json_rpc_check(logger)

    if _vdsm_json_rpc is None:
        raise RuntimeError(
            "Couldn't  connect to VDSM within {timeout} seconds".format(
                timeout=VDSM_MAX_RETRY * VDSM_DELAY
            )
        )


def __vdsm_json_rpc_check(logger=None):
    global _vdsm_json_rpc

    if _vdsm_json_rpc is None:
        return

    retry = 0
    while retry < VDSM_MAX_RETRY:
        retry += 1
        try:
            _vdsm_json_rpc.Host.ping()
            # Successful ping
            return

        except client.Error:
            __log_debug(logger, 'VDSM jsonrpc connection is not ready')

        except stomp.Disconnected:
            __log_debug(logger, 'VDSM has been disconnected')
            break

        time.sleep(VDSM_DELAY)

    # VDSM is not responding, setting client to None
    _vdsm_json_rpc = None


def connect_vdsm_json_rpc(logger=None,
                          timeout=envconst.VDSCLI_SSL_TIMEOUT):
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
