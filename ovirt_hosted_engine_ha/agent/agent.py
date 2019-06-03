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
from optparse import OptionParser
import signal
import sys
import traceback

from ..lib import exceptions as ex
from . import constants
from . import hosted_engine


class Agent(object):
    def __init__(self):
        self._shutdown = False

    def run(self):
        parser = OptionParser(version=constants.FULL_PROG_NAME)
        parser.add_option("--pdb", action="store_true",
                          dest="pdb", help="start pdb in case of crash")
        parser.add_option("--cleanup", action="store_true",
                          dest="cleanup", help="purge the metadata block")
        parser.add_option("--force-cleanup", action="store_true",
                          dest="force_cleanup", help="purge the metadata block"
                                                     "even when not clean")
        parser.add_option("--host-id", action="store", default=None,
                          type="int", dest="host_id",
                          help="override the host id")

        (options, args) = parser.parse_args()

        def action_proper(he):
            return he.start_monitoring()

        def action_clean(he):
            return he.clean(options.force_cleanup)

        action = action_proper
        errcode = 0

        if options.cleanup:
            action = action_clean

        self._initialize_logging()
        self._log.info("%s started", constants.FULL_PROG_NAME)

        self._initialize_signal_handlers()

        try:
            self._log.debug("Running agent")
            errcode = self._run_agent(action, options.host_id)

        except Exception as e:
            self._log.critical("Could not start ha-agent", exc_info=True)
            print("Could not start ha-agent: {0} (see log for details)"
                  .format(str(e)), file=sys.stderr)
            if options.pdb:
                import pdb
                pdb.post_mortem()
            sys.exit(-98)
        except KeyboardInterrupt:
            if options.pdb:
                import pdb
                pdb.post_mortem()

        # Agent shutdown...
        self._log.info("Agent shutting down")
        sys.exit(errcode)

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
            log = logging.getLogger("%s.Agent" % __name__)
            log.warn("Could not inititialize logging", exc_info=True)
        self._log = logging.getLogger("%s.Agent" % __name__)

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

    def _run_agent(self, action, host_id=None):
        # Only one service type for now, run it in the main thread

        try:
            he = hosted_engine.HostedEngine(self.shutdown_requested,
                                            host_id=host_id)

            # if we're here, the agent stopped gracefully
            return action(he)

        except hosted_engine.ServiceNotUpException as e:
            self._log.error("Service %s is not running and the admin"
                            " is responsible for starting it." % e.message)
        except ex.DisconnectionError as e:
            self._log.error("Disconnected from broker '{0}'".format(str(e)))
        except (ex.BrokerInitializationError, ex.BrokerConnectionError)\
                as e:
            self._log.error("Can't initialize brokerlink '{0}'".format(str(e)))
        except ex.StorageDisconnectedError as e:
            self._log.error("Storage disconnected '{0}'".format(str(e)))
        except Exception:
            self._log.error(traceback.format_exc())
            self._log.error("Trying to restart agent")

        return -99
