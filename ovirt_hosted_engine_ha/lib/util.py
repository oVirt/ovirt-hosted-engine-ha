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
import os
import socket
import time

from ovirt_hosted_engine_ha.env import constants as envconst
from .exceptions import DisconnectionError


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
    try:
        if isTimed is None:
            # No timeout
            sockfile = sock.makefile()
            msg = sockfile.readline()
        else:
            # Reading a line with timeout
            msg = ""
            rcvChar = 0
            sock.settimeout(timeoutSec)
            while rcvChar != '\n':
                rcvChar = sock.recv(1)
                msg = msg + rcvChar

    except socket.timeout:
        log.debug("Connection timeout while reading from socket")
        raise DisconnectionError("Connection timed out")

    except IOError as e:
        log.debug("Connection closed while reading from socket: %s", str(e))
        raise DisconnectionError("Connection closed")

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
