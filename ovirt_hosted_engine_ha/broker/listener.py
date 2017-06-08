#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2013-2014 Red Hat, Inc.
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

import errno
import logging
import os
import shlex
import socket
import SocketServer
import threading

from ..env import constants
from ..lib import util
from ..lib.exceptions import DisconnectionError
from ..lib.exceptions import RequestError
from . import notifications


class Listener(object):
    def __init__(self, monitor_instance, storage_broker_instance):
        """
        Prepare the listener and associated locks
        """
        self._log = logging.getLogger("%s.Listener" % __name__)
        self._log.info("Initializing SocketServer")

        # Coordinate access to resources across connections
        self._conn_monitors = {}
        self._conn_monitors_access_lock = threading.Lock()
        self._monitor_instance = monitor_instance
        self._monitor_instance_access_lock = threading.Lock()
        self._storage_broker_instance = storage_broker_instance
        self._storage_broker_instance_access_lock = threading.Lock()
        self._need_exit = False

        self._remove_socket_file()
        self._server = ThreadedStreamServer(
            constants.BROKER_SOCKET_FILE, ConnectionHandler, True, self)

        self._log.info("SocketServer ready")

    @property
    def conn_monitors(self):
        return self._conn_monitors

    @property
    def conn_monitors_access_lock(self):
        return self._conn_monitors_access_lock

    @property
    def monitor_instance(self):
        return self._monitor_instance

    @property
    def monitor_instance_access_lock(self):
        return self._monitor_instance_access_lock

    @property
    def storage_broker_instance(self):
        return self._storage_broker_instance

    @property
    def storage_broker_instance_access_lock(self):
        return self._storage_broker_instance_access_lock

    @property
    def need_exit(self):
        return self._need_exit

    def listen(self):
        """
        Listen briefly and return to the main loop.
        """
        # Using SocketServer.server_forever() is tempting, but it's easier to
        # shut down cleanly (and with less chance of deadlock) by returning
        # control to a main loop which checks for exit requests.
        self._server.handle_request()

    def close_connections(self):
        """
        Signal to all connection handlers that it's time to exit.
        This function must remain re-entrant.
        """
        self._need_exit = True

    def clean_up(self):
        """
        Perform final listener cleanup.  This is effectively the complement
        of __init__(), to be called after the final call to listen() and
        close_connections() have been made.
        """
        self._remove_socket_file()

    def _remove_socket_file(self):
        if os.path.exists(constants.BROKER_SOCKET_FILE):
            os.unlink(constants.BROKER_SOCKET_FILE)


class ThreadedStreamServer(SocketServer.ThreadingMixIn,
                           SocketServer.UnixStreamServer):
    def __init__(self, socket_file, handler, bind_and_activate, listener):
        # Coordinate access to resources across connections through caller
        self.sp_listener = listener

        SocketServer.UnixStreamServer.__init__(self, socket_file, handler,
                                               bind_and_activate)
        self.timeout = 0.7


class ConnectionHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        """
        Overridden method.
        Initialize connection handler class and identify connection.
        """
        if not hasattr(self, '_log'):
            self._log = logging.getLogger("%s.ConnectionHandler" % __name__)
        self._log.info("Connection established")
        self.request.settimeout(0.7)
        SocketServer.BaseRequestHandler.setup(self)
        self._init_monitors_for_conn()

    def recover(self, e):
        """
        Try to recover from exception e by reporting failure to the agent.
        Returns True if the recovery was successful and False otherwise.
        """
        response = "failure %s" % type(e)
        try:
            util.socket_sendline(self.request, self._log, response)
            return True
        except Exception:
            # we failed sending the exception report.. oops bad bad
            self._log.error("Error sending exception notification",
                            exc_info=True)
            return False

    def handle(self):
        """
        Overridden method.
        Handle an incoming connection.
        """
        while not self.server.sp_listener.need_exit:
            data = None
            try:
                data = util.socket_readline(self.request, self._log)
                self._log.debug("Input: %s", data)
                try:
                    ret = self._dispatch(data)
                    response = "success " + ret
                except RequestError as e:
                    response = "failure " + format(str(e))
                self._log.debug("Response: %s", response)
                util.socket_sendline(self.request, self._log, response)
            except socket.timeout:
                pass
            except socket.error as e:
                if e.errno == errno.EPIPE:
                    self._log.info("Connection closed")
                    return
                else:
                    # non-fatal? socket exception, try to recover by reporting
                    # a failure to the agent
                    self._log.error("Error while serving connection",
                                    exc_info=True)
                    if not self.recover(e):
                        return
            except DisconnectionError:
                self._log.info("Connection closed")
                return
            except Exception as e:
                # socket unrelated exception, report failure to the agent
                # log everything and wait for another command
                self._log.error("Error handling request, data: %r",
                                data, exc_info=True)
                if not self.recover(e):
                    return

        if self.server.sp_listener.need_exit:
            self._log.info("Closing connection on server request")

    def finish(self):
        """
        Overridden method.
        Silence exception when a connection is closed remotely.
        """
        local_monitors = self._get_monitors_for_conn()
        for id in local_monitors:
            try:
                with self.server.sp_listener.monitor_instance_access_lock:
                    self.server.sp_listener.monitor_instance \
                        .stop_submonitor(id)
            except Exception as e:
                self._log.error("Could not close submonitor on disconnection:"
                                + "%d - %s", id, str(e))
        self._remove_monitor_conn_entry()

        # cleanup storage
        with self.server.sp_listener.storage_broker_instance_access_lock:
            self.server.sp_listener.storage_broker_instance \
                .cleanup(threading.current_thread().ident)

        try:
            SocketServer.BaseRequestHandler.finish(self)
        except socket.error as e:
            if e.errno != errno.EPIPE:
                self._log.error("Error while closing connection",
                                exc_info=True)

    def _dispatch(self, data):
        """
        Parses and dispatches a request to the appropriate subsystem.

        Input is expected to be a pipe-delmited string:
            <keyword>[ <arg1[=val1]>[ <arg2[=val2]>[...]]]
        On success, output is returned as a string.
        On failure, a RequestError exception is raised (often by dispatchees).
        """
        tokens = shlex.split(data)
        type = tokens.pop(0)
        self._log.debug("Request type %s", type)

        # TODO fix to be less procedural, e.g. dict of req_type=handler_func()
        if type == 'monitor':
            # Start a submonitor and add its id to the list at this
            # thread's slot of the self.server.sp_listener.conn_monitors dict
            submonitor = tokens.pop(0)
            options = self._get_options(tokens)
            with self.server.sp_listener.monitor_instance_access_lock:
                id = self.server.sp_listener.monitor_instance \
                    .start_submonitor(submonitor, options)
            self._add_monitor_for_conn(id)
            return str(id)
        elif type == 'stop-monitor':
            # Stop a submonitor and remove it from the conn_monitors list
            id = int(tokens.pop(0))
            with self.server.sp_listener.monitor_instance_access_lock:
                status = self.server.sp_listener.monitor_instance \
                    .stop_submonitor(id)
            self._remove_monitor_for_conn(id)
            return "ok"
        elif type == 'status':
            id = int(tokens.pop(0))
            with self.server.sp_listener.monitor_instance_access_lock:
                status = self.server.sp_listener.monitor_instance.get_value(id)
            return str(status)
        elif type == 'get-stats':
            options = self._get_options(tokens)
            with self.server.sp_listener.storage_broker_instance_access_lock:
                stats = self.server.sp_listener.storage_broker_instance \
                    .get_all_stats_for_service_type(**options)
            return stats
        elif type == 'push-hosts-state':
            options = self._get_options(tokens)
            with self.server.sp_listener.storage_broker_instance_access_lock:
                self.server.sp_listener.storage_broker_instance \
                    .push_hosts_state(**options)
            return "ok"
        elif type == 'is-host-alive':
            options = self._get_options(tokens)
            with self.server.sp_listener.storage_broker_instance_access_lock:
                alive_hosts = self.server.sp_listener.storage_broker_instance \
                    .is_host_alive(**options)
            # list of alive hosts in format <host_id>|<host_id>
            return alive_hosts
        elif type == 'put-stats':
            options = self._get_options(tokens)
            with self.server.sp_listener.storage_broker_instance_access_lock:
                self.server.sp_listener.storage_broker_instance \
                    .put_stats(**options)
            return "ok"
        elif type == 'service-path':
            service = tokens.pop(0)
            with self.server.sp_listener.storage_broker_instance_access_lock:
                return self.server.sp_listener.storage_broker_instance \
                    .get_service_path(service)
        elif type == 'set-storage-domain':
            sd_type = tokens.pop(0)
            options = self._get_options(tokens)
            with self.server.sp_listener.storage_broker_instance_access_lock:
                return self.server.sp_listener.storage_broker_instance \
                    .set_storage_domain(sd_type, **options)
        elif type == 'notify':
            options = self._get_options(tokens)
            if notifications.notify(**options):
                return "sent"
            else:
                return "ignored"
        else:
            self._log.error("Unrecognized request: %s", data)
            raise RequestError("unrecognized request")

    def _get_options(self, tokens):
        """
        Convert a list of key=value pairs to a dictionary.  The leftmost '='
        encountered is used to split the string.
        """
        options = {}
        for t in tokens:
            try:
                (arg, value) = t.split('=', 1)
            except ValueError:
                raise RequestError("Malformed command option: {0}".format(t))
            options[arg] = value
        return options

    def _init_monitors_for_conn(self):
        """
        This function (and others related) assume each connection corresponds
        to a thread and uses this to track monitors accordingly.
        """
        with self.server.sp_listener.conn_monitors_access_lock:
            self.server.sp_listener.conn_monitors[
                threading.current_thread().ident
            ] = []

    def _get_monitors_for_conn(self):
        with self.server.sp_listener.conn_monitors_access_lock:
            ret = self.server.sp_listener.conn_monitors[
                threading.current_thread().ident
            ]
        return ret

    def _add_monitor_for_conn(self, id):
        with self.server.sp_listener.conn_monitors_access_lock:
            self.server.sp_listener.conn_monitors[
                threading.current_thread().ident
            ].append(id)

    def _remove_monitor_for_conn(self, id):
        with self.server.sp_listener.conn_monitors_access_lock:
            self.server.sp_listener.conn_monitors[
                threading.current_thread().ident
            ].remove(id)

    def _remove_monitor_conn_entry(self):
        with self.server.sp_listener.conn_monitors_access_lock:
            del self.server.sp_listener.conn_monitors[
                threading.current_thread().ident
            ]
