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
import logging
import logging.config
from optparse import OptionParser
import os
import signal
import sys

from ..lib import util
from . import constants
from . import hosted_engine


class Agent(object):
    def __init__(self):
        self._shutdown = False

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

            self._log.debug("Writing pid file")
            util.mkdir_recursive(os.path.dirname(constants.PID_FILE))
            with open(constants.PID_FILE, 'w') as f:
                f.write(str(os.getpid()) + "\n")

            # FIXME exit if another ha-agent instance is already running...
            # can use python-lockfile since it's already available

            if options.daemon:
                self._log.debug("Running agent as daemon")
                # The logger expects its file descriptors to stay open
                logs = [x.stream for x in logging.getLogger().handlers
                        if hasattr(x, "stream")]
                logs.extend([x.socket for x in logging.getLogger().handlers
                             if hasattr(x, "socket")])
                self._log.debug("Preserving {0}".format(logs))
                with daemon.DaemonContext(signal_map=self._get_signal_map(),
                                          files_preserve=logs):
                    self._run_agent()
            else:
                self._log.debug("Running agent in foreground")
                self._run_agent()

        except Exception as e:
            self._log.critical("Could not start ha-agent", exc_info=True)
            print("Could not start ha-agent: {0} (see log for details)"
                  .format(str(e)), file=sys.stderr)
            sys.exit(1)

        # Agent shutdown...
        self._log.info("Agent shutting down")
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
        self._shutdown = True

    def shutdown_requested(self):
        return self._shutdown

    def _run_agent(self):
        # Only one service type for now, run it in the main thread
        hosted_engine.HostedEngine(self.shutdown_requested).start_monitoring()
