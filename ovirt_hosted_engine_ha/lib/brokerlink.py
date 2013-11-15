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

import base64
import contextlib
import logging
import socket
import time

from ..env import constants
from ..lib.exceptions import DisconnectionError
from ..lib.exceptions import RequestError
from ..lib import util


class BrokerLink(object):
    def __init__(self):
        self._log = logging.getLogger("BrokerLink")
        self._socket = None

    def connect(self, retries=0):
        """
        Connect to the HA Broker.  Upon failure, reconnection attempts will
        be made approximately once per second until the specified number of
        retries have been made.  An exception will be raised if a connection
        cannot be established.
        """
        if self.is_connected():
            return
        self._log.debug("Connecting to ha-broker")

        try:
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        except socket.error as e:
            self._log.error("Failed to connect to broker: %s", str(e))
            if self._socket:
                self._socket.close()
            self._socket = None
            raise

        attempt = 0
        while True:
            try:
                self._socket.connect(constants.BROKER_SOCKET_FILE)
            except socket.error as e:
                if attempt < retries:
                    self._log.info("Failed to connect to broker: %s", str(e))
                    self._log.info("Retrying broker connection...")
                    time.sleep(1)
                    continue
                else:
                    self._log.error("Failed to connect to broker: %s", str(e))
                    self._socket.close()
                    self._socket = None
                    raise
            self._log.debug("Successfully connected")
            break

    def is_connected(self):
        return self._socket is not None

    def disconnect(self):
        self._log.debug("Closing connection to ha-broker")
        try:
            if self._socket:
                self._socket.close()
        except socket.error:
            self._log.info("Socket error closing connection")
        finally:
            self._socket = None

    @contextlib.contextmanager
    def connection(self):
        was_connected = self.is_connected()
        if not was_connected:
            self.connect()
        yield
        if not was_connected:
            self.disconnect()

    def start_monitor(self, type, options):
        """
        Starts a monitor of the specified type in the ha broker using the
        given options dictionary, returning an id on success.
        """
        self._log.info("Starting monitor {0}, options {1}"
                       .format(type, options))
        request = "monitor {0}".format(type)
        for (k, v) in options.iteritems():
            request += " {0}={1}".format(k, str(v))

        try:
            response = self._checked_communicate(request)
        except Exception as e:
            raise RequestError("Failed to start monitor {0}, options {1}: {2}"
                               .format(type, options, e))

        self._log.info("Success, id %s", response)
        return response

    def get_monitor_status(self, id):
        request = "status {0}".format(id)
        try:
            response = self._checked_communicate(request)
        except Exception as e:
            self._log.error("Exception getting monitor status: %s", str(e))
            raise RequestError("Failed to get monitor status: {0}"
                               .format(str(e)))
        self._log.debug("Success, status %s", response)
        return response

    def put_stats_on_storage(self, storage_dir, service_type, host_id, data):
        """
        Puts data on the shared storage according to the parameters.
        Data should be passed in as a string.
        """
        self._log.debug("Storing blocks on storage at %s", storage_dir)
        # broker expects blocks in hex format
        hex_data = base64.b16encode(data)
        request = ("put-stats"
                   " storage_dir={0} service_type={1} host_id={2} data={3}"
                   .format(storage_dir, service_type, host_id, hex_data))
        self._checked_communicate(request)

    def get_stats_from_storage(self, storage_dir, service_type):
        """
        Returns data from the shared storage for all hosts of the specified
        service type.
        """
        request = ("get-stats storage_dir={0} service_type={1}"
                   .format(storage_dir, service_type))
        result = self._checked_communicate(request)

        tokens = result.split()
        ret = {}
        # broker returns "<host_id 1>=<hex data 1> [<host_id 2>=...]"
        while tokens:
            (host_id, data) = tokens.pop(0).split('=', 1)
            ret[int(host_id)] = base64.b16decode(data)
        return ret

    def _checked_communicate(self, request):
        """
        Wrapper around _communicate() which checks for success/failure.
        On failure, throws an exception with the failure message.
        On success, returns the result without the "success" prefix.
        """
        response = self._communicate(request)
        parts = response.split(" ", 1)
        if len(parts) > 1 and parts[0] == "success":
            self._log.debug("Successful response from socket")
            return parts[1]
        else:
            self._log.debug("Failed response from socket")
            if len(parts) > 1:
                msg = parts[1]
            else:
                msg = response
            raise RequestError("Request failed: {0}".format(msg))

    def _communicate(self, request):
        """
        Sends a request to the (already-connected) ha-broker, waits for a
        response, and returns it to the caller.  Both the request and
        response are non-newline-terminated strings.

        On disconnection, raises DisconnectionError.
        """
        if not self.is_connected():
            raise DisconnectionError("Not connected to broker")

        self._log.debug("Sending request: %s", request)
        try:
            util.socket_sendline(self._socket, self._log, request)
            response = util.socket_readline(self._socket, self._log)
        except DisconnectionError:
            self._log.error("Connection closed")
            self.disconnect()
            raise
        self._log.debug("Full response: %s", response)
        return response
