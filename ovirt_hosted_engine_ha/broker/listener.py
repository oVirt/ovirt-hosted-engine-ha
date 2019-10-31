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
try:
    import xmlrpc.client as xmlrpc_client
except ImportError:
    import xmlrpclib as xmlrpc_client

from functools import wraps

from ..env import constants
from ..lib import unixrpc
from . import notifications


class Listener(object):
    def __init__(self, monitor_instance, storage_broker_instance,
                 status_broker_instance):
        """
        Prepare the listener and associated locks
        """
        threading.current_thread().name = "Listener"
        self._log = logging.getLogger("{}.Listener".format(__name__))
        self._log.info("Initializing RPCServer")

        # Coordinate access to resources across connections
        self._monitor_instance = monitor_instance
        self._storage_broker_instance = storage_broker_instance
        self._status_broker_instance = status_broker_instance

        self._actions = ActionsHandler(self)

        self._remove_socket_file()
        self._server = unixrpc.UnixXmlRpcServer(constants.BROKER_SOCKET_FILE)
        self._server.register_instance(self._actions)

        self._log.info("RPCServer ready")

    @property
    def monitor_instance(self):
        return self._monitor_instance

    @property
    def storage_broker_instance(self):
        return self._storage_broker_instance

    @property
    def status_broker_instance(self):
        return self._status_broker_instance

    def listen(self):
        """
        Listen and block
        """
        self._server.serve_forever()

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


def logged(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        _log = logging.getLogger("{}.Action.{}".format(__name__, f.__name__))
        try:
            _log.debug("Executing RPC handler %s with params %s",
                       f.__name__, str(args))
            return f(*args, **kwds)
        except Exception as e:
            _log.error("Error in RPC call: {}".format(str(e)))
            _log.debug("Traceback:", exc_info=1)

    return wrapper


class ActionsHandler(object):
    def __init__(self, listener):
        self._log = logging.getLogger("{}.ActionsHandler".format(__name__))

        self._listener = listener
        self._monitor_instance_access_lock = threading.Lock()
        self._storage_broker_instance_access_lock = threading.Lock()
        self._status_broker_instance_access_lock = threading.Lock()

    @logged
    def start_monitor(self, type, options):
        with self._monitor_instance_access_lock:
            id = self._listener.monitor_instance \
                .start_submonitor(type, options)
        return id

    @logged
    def stop_monitor(self, id):
        # Stop a submonitor and remove it from the conn_monitors list
        with self._monitor_instance_access_lock:
            self._listener.monitor_instance.stop_submonitor(id)
        return "ok"

    @logged
    def status_monitor(self, id):
        with self._monitor_instance_access_lock:
            status = self._listener.monitor_instance.get_value(id)
        return str(status)

    @logged
    def get_stats(self):
        result = {}
        with self._status_broker_instance_access_lock:
            state = self._listener.status_broker_instance.get_stats()

        # As raw binary data can not be transferred via xmlrpc link,
        # we need to wrap state's contents into Binary wrapper.
        for k, v in six.iteritems(state):
            result[k] = xmlrpc_client.Binary(v.encode())
        return result

    @logged
    def push_hosts_state(self, alive_hosts):
        with self._storage_broker_instance_access_lock:
            self._listener.storage_broker_instance \
                .push_hosts_state(alive_hosts)
        return "ok"

    @logged
    def is_host_alive(self):
        with self._storage_broker_instance_access_lock:
            alive_hosts = self._listener.storage_broker_instance \
                .is_host_alive()
        # list of alive hosts in format <host_id>|<host_id>
        return alive_hosts

    @logged
    def put_stats(self, host_id, data):
        with self._status_broker_instance_access_lock:
            self._listener.status_broker_instance \
                .put_stats(data, host_id=host_id)
        return "ok"

    @logged
    def notify(self, event_type, detail, options):
        if notifications.notify(event_type, detail, options):
            return "sent"
        else:
            return "ignored"

    @logged
    def start_domain_monitor(self, host_id):
        with self._storage_broker_instance_access_lock:
            self._listener.storage_broker_instance \
                .start_domain_monitor(host_id)
        return "ok"

    @logged
    def stop_domain_monitor(self):
        with self._storage_broker_instance_access_lock:
            self._listener.storage_broker_instance \
                .stop_domain_monitor()
        return "ok"

    @logged
    def lock_host_id(self, host_id):
        with self._status_broker_instance_access_lock:
            self._listener.status_broker_instance.lock_host_id(host_id)
            return "ok"

    @logged
    def release_host_id(self):
        with self._status_broker_instance_access_lock:
            self._listener.status_broker_instance.release_host_id()
            return "ok"

    @logged
    def reset_lockspace(self):
        with self._status_broker_instance_access_lock:
            self._listener.status_broker_instance.reset_lockspace()
        return "ok"
