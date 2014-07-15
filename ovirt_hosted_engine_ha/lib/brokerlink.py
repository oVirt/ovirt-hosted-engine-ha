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
from ..lib.exceptions import BrokerConnectionError
from ..lib import util


class NotifyEvents(object):
    STATE_TRANSITION = "state_transition"


class BrokerLink(object):
    def __init__(self):
        self._log = logging.getLogger("%s.BrokerLink" % __name__)
        self._socket = None

    def connect(self, retries=0, wait=5):
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

        for attempt in range(retries):
            try:
                self._socket.connect(constants.BROKER_SOCKET_FILE)
                break
            except socket.error as e:
                self._log.info("Failed to connect to broker: %s", str(e))
                self._log.info("Retrying broker connection in '{0}' seconds"
                               .format(wait))
                time.sleep(wait)
        else:
            error_msg = ("Failed to connect to broker, the number of "
                         "errors has exceeded the limit ({0})"
                         .format(retries))
            self._log.error(error_msg)
            self._socket.close()
            self._socket = None
            raise BrokerConnectionError(error_msg)

        self._log.debug("Successfully connected")

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

    def notify(self, event_type, detail, **options):
        request = ["notify time={0} type={1} detail={2}"
                   .format(time.time(), event_type, detail, "description")]
        for k, v in options.iteritems():
            request.append("{0}={1}".format(k, repr(v)))
        request = " ".join(request)

        self._log.info("Trying: %s", request)

        try:
            response = self._checked_communicate(request)
        except Exception as e:
            raise RequestError("Failed to start monitor {0}, options {1}: {2}"
                               .format(type, options, e))

        self._log.info("Success, was notification of "
                       "%s (%s) sent? %s", event_type, detail, response)
        return response

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

    def get_service_path(self, service):
        request = "service-path {0}".format(service)
        try:
            response = self._checked_communicate(request)
        except Exception as e:
            self._log.error("Exception getting service path: %s", str(e))
            raise RequestError("Failed to get service path: {0}"
                               .format(str(e)))
        self._log.debug("Success, service path %s", response)
        return response

    def set_storage_domain(self, sd_type, **options):
        request = ["set-storage-domain {0}".format(sd_type)]
        for (k, v) in options.iteritems():
            request.append("{0}={1}".format(k, str(v)))
        request = " ".join(request)

        try:
            response = self._checked_communicate(request)
        except Exception as e:
            raise RequestError("Failed to set storage domain {0}, "
                               "options {1}: {2}"
                               .format(sd_type, options, e))

        self._log.info("Success, id %s", response)
        return response

    def put_stats_on_storage(self, service_type, host_id, data):
        """
        Puts data on the shared storage according to the parameters.
        Data should be passed in as a string.
        """
        self._log.debug("Storing blocks on storage for %s", service_type)
        # broker expects blocks in hex format
        hex_data = base64.b16encode(data)
        request = ("put-stats"
                   " service_type={0} host_id={1} data={2}"
                   .format(service_type, host_id, hex_data))
        self._checked_communicate(request)

    def put_hosts_state_on_storage(self, service_type, host_id, alive_hosts):
        """
        Broker expects list of alive hosts in format:
        <host_id>|<host_id>
        The broker adds it's own monotonic timestamp before saving it
        """

        # since we're reporting it we are alive ;)
        _alive_hosts = [host_id] + alive_hosts
        host_str = "|".join(str(host_id) for host_id in _alive_hosts)

        self._log.debug("Updating live hosts list")
        hex_data = base64.b16encode(host_str)
        request = ("push-hosts-state"
                   " service_type={0} data={1}"
                   .format(service_type, hex_data))
        self._checked_communicate(request)

    def is_host_alive(self, service_type, host_id):
        request = ("is-host-alive service_type={0}"
                   .format(service_type))
        response = self._checked_communicate(request)
        if not response:
            return False

        host_list = map(int, base64.b16decode(response).split('|'))
        self._log.debug("Alive hosts '{0}'".format(host_list))
        self._log.debug("Is host '{0}' alive -> '{1}'"
                        .format(host_id, host_id in host_list))
        return host_id in host_list

    def get_stats_from_storage(self, service_type):
        """
        Returns data from the shared storage for all hosts of the specified
        service type.
        """
        request = ("get-stats service_type={0}"
                   .format(service_type))
        result = self._checked_communicate(request)
        tokens = result.split()
        ret = {}
        # result is in form: "<host_id 1>=<hex data 1> [<host_id 2>=...]"
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
        try:
            status, message = response.split(" ", 1)
        except ValueError:
            status, message = response, ""

        if status == "success":
            self._log.debug("Successful response from socket")
            return message
        else:
            self._log.debug("Failed response from socket")
            raise RequestError("Request failed: {0}"
                               .format(message or response))

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
