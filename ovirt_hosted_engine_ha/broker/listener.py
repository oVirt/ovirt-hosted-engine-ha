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

import logging
import os
import six
import threading

from ..env import constants
from ..lib import unixrpc
from . import notifications


class Listener(object):
    def __init__(self, monitor_instance, storage_broker_instance):
        """
        Prepare the listener and associated locks
        """
        threading.current_thread().name = "Listener"
        self._log = logging.getLogger("%s.Listener" % __name__)
        self._log.info("Initializing RPCServer")

        # Coordinate access to resources across connections
        self._conn_monitors = []
        self._monitor_instance = monitor_instance
        self._storage_broker_instance = storage_broker_instance

        self._actions = ActionsHandler(self)

        self._remove_socket_file()
        self._server = unixrpc.UnixXmlRpcServer(constants.BROKER_SOCKET_FILE)
        for name, func in six.iteritems(self._actions.mappings):
            self._server.register_function(func, name)

        self._log.info("RPCServer ready")

    @property
    def conn_monitors(self):
        return self._conn_monitors

    @property
    def actions(self):
        return self._actions

    @property
    def monitor_instance(self):
        return self._monitor_instance

    @property
    def storage_broker_instance(self):
        return self._storage_broker_instance

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
        self._server.shutdown()

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


class ActionsHandler(object):
    def __init__(self, listener):
        self._log = logging.getLogger("%s.ActionsHandler" % __name__)

        self._listener = listener
        self._conn_monitors_access_lock = threading.Lock()
        self._monitor_instance_access_lock = threading.Lock()
        self._storage_broker_instance_access_lock = threading.Lock()

        self._dispatcher = {
            'start_monitor': self._handle_monitor,
            'stop_monitor': self._handle_stop_monitor,
            'status': self._handle_status,
            'get_stats': self._handle_get_stats,
            'push_hosts_state': self._handle_push_host_state,
            'is_host_alive': self._handle_is_host_alive,
            'put_stats': self._handle_put_stats,
            'service_path': self._handle_service_path,
            'notify': self._handle_notify
        }

    @property
    def mappings(self):
        return self._dispatcher

    def _handle_monitor(self, type, options):
        with self._monitor_instance_access_lock:
            id = self._listener.monitor_instance \
                .start_submonitor(type, options)
        self._add_monitor_for_conn(id)
        return id

    def _handle_stop_monitor(self, tokens):
        # Stop a submonitor and remove it from the conn_monitors list
        id = int(tokens.pop(0))
        with self._monitor_instance_access_lock:
            self._listener.monitor_instance.stop_submonitor(id)
        self._remove_monitor_for_conn(id)
        return "ok"

    def _handle_status(self, id):
        with self._monitor_instance_access_lock:
            status = self._listener.monitor_instance.get_value(id)
        return str(status)

    def _handle_get_stats(self, service_type):
        with self._storage_broker_instance_access_lock:
            stats = self._listener.storage_broker_instance \
                .get_all_stats_for_service_type(service_type)
        return stats

    def _handle_push_host_state(self, service_type, alive_hosts):
        with self._storage_broker_instance_access_lock:
            self._listener.storage_broker_instance \
                .push_hosts_state(service_type, alive_hosts)
        return "ok"

    def _handle_is_host_alive(self, service_type):
        with self._storage_broker_instance_access_lock:
            alive_hosts = self._listener.storage_broker_instance \
                .is_host_alive(service_type)
        # list of alive hosts in format <host_id>|<host_id>
        return alive_hosts

    def _handle_put_stats(self, service_type, host_id, data):
        with self._storage_broker_instance_access_lock:
            self._listener.storage_broker_instance \
                .put_stats(service_type, host_id, data)
        return "ok"

    def _handle_service_path(self, service):
        with self._storage_broker_instance_access_lock:
            return self._listener.storage_broker_instance \
                .get_service_path(service)

    def _handle_notify(self, event_type, detail, options):
        if notifications.notify(event_type, detail, options):
            return "sent"
        else:
            return "ignored"

    def _add_monitor_for_conn(self, id):
        with self._conn_monitors_access_lock:
            self._listener.conn_monitors.append(id)

    def _remove_monitor_for_conn(self, id):
        with self._conn_monitors_access_lock:
            self._listener.conn_monitors.remove(id)
