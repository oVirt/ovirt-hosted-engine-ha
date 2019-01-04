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

from __future__ import print_function

import ConfigParser
import logging
import logging.config
import os
import signal
import sys
import threading
import time

from . import constants
from . import listener
from . import monitor
from . import status_broker
from . import storage_broker


class Broker(object):
    def __init__(self):
        self._listener = None
        self._monitor_instance = None
        self._storage_broker_instance = None
        self._status_broker_instance = None

    def run(self):
        self._initialize_logging()
        self._log.info("%s started", constants.FULL_PROG_NAME)

        self._initialize_signal_handlers()

        """
        Performs setup and execution of main server code, encompassing a
        monitor manager, storage broker, and request listener.
        """
        self._log.debug("Running broker")
        self._monitor_instance = self._get_monitor()
        self._storage_broker_instance = self._get_storage_broker()
        self._status_broker_instance = self._get_status_broker()
        self._listener = self._get_listener()
        self._listener.listen()

        # Server shutdown...
        self._log.info("Server shutting down")
        self._listener.clean_up()
        self._monitor_instance.stop_all_submonitors()
        self._status_broker_instance.clean_up()
        sys.exit(0)

    def _initialize_logging(self):
        try:
            logging.config.fileConfig(constants.LOG_CONF_FILE,
                                      disable_existing_loggers=False)
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(logging.Formatter(
                "%(levelname)s:%(name)s:%(message)s"))
            logging.getLogger('').addHandler(handler)
        except (ConfigParser.Error, ImportError, NameError, TypeError):
            logging.basicConfig(filename='/dev/stdout', filemode='w+',
                                level=logging.DEBUG)
            log = logging.getLogger("%s.Broker" % __name__)
            log.warn("Could not inititialize logging", exc_info=True)
        self._log = logging.getLogger("%s.Broker" % __name__)

    def _get_signal_map(self):
        return {signal.SIGINT: self._handle_quit,
                signal.SIGTERM: self._handle_quit}

    def _initialize_signal_handlers(self):
        for signum, handler in self._get_signal_map().iteritems():
            signal.signal(signum, handler)

    def _handle_quit(self, signum, frame):
        # xmlrpc server shutdown should not be called from thread,
        # that called serve_forever. Unfortunately, due to python
        # bug, it is not possible to run serve_forever() on the other thread
        # and wait for it in the main thread, cause it will block singal
        # processing. Therefore, i had to start a new thread directly
        # in a signal handler and call shutdown from that thread.
        down_thread = threading.Thread(target=self._listener.close_connections)
        down_thread.daemon = True
        down_thread.start()

        # If the process doesn't finish in a reasonable time, exit
        # unconditionally. Otherwise the listener may stop processing
        # requests while still listening on its socket, resulting in
        # hanging requests.
        def wait_and_exit():
            time.sleep(30)
            os._exit(0)
        exit_thread = threading.Thread(target=wait_and_exit)
        exit_thread.daemon = True
        exit_thread.start()

    def _get_monitor(self):
        """
        Starts monitor manager, which provides centralized control of various
        plug-in submonitors through which system status can be read.
        """
        self._log.debug("Starting monitor")
        return monitor.Monitor()

    def _get_storage_broker(self):
        """
        Starts storage broker, which providing an interface to read/write
        data that applies to various hosts and service types.
        """
        self._log.debug("Starting storage broker")
        return storage_broker.StorageBroker()

    def _get_status_broker(self):
        self._log.debug("Starting status broker")
        return status_broker.StatusBroker(self._storage_broker_instance)

    def _get_listener(self):
        """
        Returns request listener used in main loop to serve requests.
        """
        self._log.debug("Starting listener")
        return listener.Listener(self._monitor_instance,
                                 self._storage_broker_instance,
                                 self._status_broker_instance)
