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
import signal
import sys
import threading

from . import constants
from . import listener
from . import monitor
from . import storage_broker


class Broker(object):
    def __init__(self):
        self._listener = None
        self._monitor_instance = None
        self._storage_broker_instance = None

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
        self._listener = self._get_listener()
        self._listener.listen()

        # Server shutdown...
        self._log.info("Server shutting down")
        self._listener.clean_up()
        self._monitor_instance.stop_all_submonitors()
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
        threading.Thread(target=self._listener.close_connections).start()

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

    def _get_listener(self):
        """
        Returns request listener used in main loop to serve requests.
        """
        self._log.debug("Starting listener")
        return listener.Listener(self._monitor_instance,
                                 self._storage_broker_instance)
