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

from .exceptions import DisconnectionError


def mkdir_recursive(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def socket_readline(sock, log):
    """
    Reads from a socket until newline is received.  Returns string read
    (without trailing newline), or raises either DisconnectionError on
    disconnect or socket.timeout on timeout.
    """
    try:
        sockfile = sock.makefile()
        msg = sockfile.readline()
    except socket.timeout:
        raise
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
        raise Exception("Invalid value for boolean: {0}".format(string))
