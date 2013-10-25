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
import daemon
import errno
import grp
import logging
import logging.config
from optparse import OptionParser
import os
import pwd
import select
import signal
import sys

from ..lib import util
from . import constants
from . import listener
from . import monitor
from . import storage_broker


class Broker(object):
    def __init__(self):
        self._listener = None
        self._monitor_instance = None
        self._storage_broker_instance = None
        self._serve_requests = False

    def run(self):
        parser = OptionParser(version=constants.FULL_PROG_NAME)
        parser.add_option("--no-daemon", action="store_false",
                          dest="daemon", help="don't start as a daemon")
        parser.set_defaults(daemon=True)
        (options, args) = parser.parse_args()

        self._initialize_logging(options.daemon)
        self._log.info("%s started", constants.FULL_PROG_NAME)

        self._initialize_signal_handlers()

        try:
            self._log.debug("Verifying execution as root")
            if os.geteuid() != 0:
                raise Exception("This program must be run as root")

            vdsm_uid = pwd.getpwnam(constants.VDSM_USER).pw_uid
            vdsm_gid = grp.getgrnam(constants.VDSM_GROUP).gr_gid

            self._log.debug("Writing pid file")
            util.mkdir_recursive(os.path.dirname(constants.PID_FILE))
            os.chown(os.path.dirname(constants.PID_FILE), vdsm_uid, vdsm_gid)
            with open(constants.PID_FILE, 'w') as f:
                f.write(str(os.getpid()) + "\n")

            # FIXME exit if another ha-broker instance is already running...
            # can use python-lockfile since it's already available

            self._log.debug("Running as %s:%s (%d:%d)",
                            constants.VDSM_USER, constants.VDSM_GROUP,
                            vdsm_uid, vdsm_gid)
            if hasattr(os, 'initgroups'):
                os.initgroups(constants.VDSM_USER, vdsm_gid)
            else:
                vdsm_gids = [g.gr_gid for g in grp.getgrall()
                             if constants.VDSM_USER in g.gr_mem]
                vdsm_gids.append(vdsm_gid)
                os.setgroups(vdsm_gids)
            os.setgid(vdsm_gid)
            os.setuid(vdsm_uid)

            if options.daemon:
                self._log.debug("Running broker as daemon")
                # The logger expects its file descriptors to stay open
                logs = [x.stream for x in logging.getLogger().handlers
                        if hasattr(x, "stream")]
                logs.extend([x.socket for x in logging.getLogger().handlers
                             if hasattr(x, "socket")])
                self._log.debug("Preserving {0}".format(logs))

                with daemon.DaemonContext(signal_map=self._get_signal_map(),
                                          files_preserve=logs):
                    self._run_server()
            else:
                self._log.debug("Running broker in foreground")
                self._run_server()

        except Exception as e:
            self._log.critical("Could not start ha-broker", exc_info=True)
            print("Could not start ha-broker: {0} (see log for details)"
                  .format(str(e)), file=sys.stderr)
            sys.exit(1)

        # Server shutdown...
        self._log.info("Server shutting down")
        self._listener.clean_up()
        os.unlink(constants.PID_FILE)
        sys.exit(0)

    def _initialize_logging(self, is_daemon):
        try:
            logging.config.fileConfig(constants.LOG_CONF_FILE,
                                      disable_existing_loggers=False)
            if not is_daemon:
                handler = logging.StreamHandler()
                handler.setLevel(logging.DEBUG)
                handler.setFormatter(logging.Formatter(
                    "%(levelname)s:%(name)s:%(message)s"))
                logging.getLogger('').addHandler(handler)
        except (ConfigParser.Error, ImportError, NameError, TypeError):
            logging.basicConfig(filename='/dev/stdout', filemode='w+',
                                level=logging.DEBUG)
            log = logging.getLogger("Broker")
            log.warn("Could not inititialize logging", exc_info=True)
        self._log = logging.getLogger("Broker")

    def _get_signal_map(self):
        return {signal.SIGINT: self._handle_quit,
                signal.SIGTERM: self._handle_quit}

    def _initialize_signal_handlers(self):
        for signum, handler in self._get_signal_map().iteritems():
            signal.signal(signum, handler)

    def _handle_quit(self, signum, frame):
        # Remain re-entrant
        self._serve_requests = False
        self._listener.close_connections()

    def _run_server(self):
        """
        Performs setup and execution of main server code, encompassing a
        monitor manager, storage broker, and request listener.
        """
        self._log.debug("Initializing server tasks")
        self._monitor_instance = self._get_monitor()
        self._storage_broker_instance = self._get_storage_broker()
        self._listener = self._get_listener(self._monitor_instance,
                                            self._storage_broker_instance)
        self._serve_requests = True
        while self._serve_requests:
            try:
                self._listener.listen()
            except select.error as e:
                if e[0] != errno.EINTR:
                    raise

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

    def _get_listener(self, monitor_instance, storage_broker_instance):
        """
        Returns request listener used in main loop to serve requests.
        """
        self._log.debug("Starting listener")
        return listener.Listener(monitor_instance, storage_broker_instance)
