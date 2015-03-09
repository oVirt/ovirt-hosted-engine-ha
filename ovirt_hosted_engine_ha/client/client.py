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

import logging
import os.path
import sanlock

from ..agent import constants as agent_constants
from ..env import config
from ..env import constants
from ..lib import brokerlink
from ..lib import metadata
from ..lib import util
from ..lib.exceptions import MetadataError
from ..lib.storage_backends import StorageBackendTypes


class HAClient(object):
    class StatModes(object):
        """
        Constants used in calls to retrieve runtime stats:
          ALL - return global metadata and host statistics
          HOST - return only host statistics
          GLOBAL - return only global metadata
        """
        ALL = 'ALL'
        HOST = 'HOST'
        GLOBAL = 'GLOBAL'

    class GlobalMdFlags(object):
        """
        Constants used to refer to global metadata flags:
          MAINTENANCE - maintenance flag
        Note that the value here must equal a key in metadata.global_flags
        """
        MAINTENANCE = 'maintenance'

    class MaintenanceMode(object):
        """
        Constants used in calls to set maintenance mode:
          LOCAL - local host maintenance
          GLOBAL - global maintenance
        """
        LOCAL = 'LOCAL'
        GLOBAL = 'GLOBAL'

    def __init__(self, log=False):
        """
        Create an instance of HAClient.  If the caller has a log handler, it
        should pass in log=True, else logging will effectively be disabled.
        """
        if not log:
            logging.basicConfig(filename='/dev/null', filemode='w+',
                                level=logging.CRITICAL)
        self._log = logging.getLogger("%s.HAClient" % __name__)
        self._config = None

    def _check_liveness_metadata(self, md, broker):
        with broker.connection():
            self._configure_broker_conn(broker)
            service = constants.SERVICE_TYPE+agent_constants.MD_EXTENSION
            md["live-data"] = broker.is_host_alive(service, md["host-id"])
            self._log.debug("Is host '{0}' alive? -> '{1}'"
                            .format(md["host-id"], md["live-data"]))

        return md["live-data"]

    def _check_liveness_for_stats(self, stats, broker):
        for host_id in stats:
            if host_id == 0:  # global stats
                continue
            self._check_liveness_metadata(stats[host_id], broker)

    def get_all_stats(self, mode=StatModes.ALL):
        """
        Connects to HA broker to get global md and/or host stats, based on
        mode (member of StatModes class).  Returns the stats in a dictionary
        as {host_id: = {key: value, ...}}
        """
        if self._config is None:
            self._config = config.Config()
        broker = brokerlink.BrokerLink()
        with broker.connection():
            self._configure_broker_conn(broker)
            service = constants.SERVICE_TYPE+agent_constants.MD_EXTENSION
            stats = broker.get_stats_from_storage(service)

        stats = self._parse_stats(stats, mode)
        self._check_liveness_for_stats(stats, broker)
        return stats

    def get_all_stats_direct(self, sd_uuid, dom_type, service_type,
                             mode=StatModes.ALL):
        """
        Like get_all_stats(), but bypasses broker by directly accessing
        storage.
        """
        from ..broker import storage_broker

        sb = storage_broker.StorageBroker()
        sb.set_storage_domain("client", StorageBackendTypes.FilesystemBackend,
                              sd_uuid=sd_uuid, dom_type=dom_type)
        stats = sb.get_raw_stats_for_service_type("client", service_type)

        return self._parse_stats(stats, mode)

    def _parse_stats(self, stats, mode):
        """
        Parse passed-in stats dict, typically returned from the HA broker.
        It should be a dictionary with key being the host id (or 0 for global
        metadata) and value being the string-encoded representation of the
        host and/or global statistics, decodable by the parsing routines in
        the metadata module.

        This returns a dict of dicts containing the parsed metadata, logging
        any encountered errors.  No mechanism is currently provided for
        callers to detect a parsing error.
        """
        output = {}
        for host_id, data in stats.iteritems():
            try:
                if host_id == 0 and mode != self.StatModes.HOST:
                    md = metadata.parse_global_metadata_to_dict(self._log,
                                                                data)
                    output[0] = md
                elif host_id != 0 and mode != self.StatModes.GLOBAL:
                    md = metadata.parse_metadata_to_dict(host_id, data)
                    output[md['host-id']] = md
                else:
                    continue
            except MetadataError as e:
                self._log.error(str(e))
                continue
        return output

    def get_all_host_stats(self):
        """
        Connects to HA broker, reads stats for all hosts, and returns
        them in a dictionary as {host_id: = {key: value, ...}}
        """
        return self.get_all_stats(self.StatModes.HOST)

    def get_all_host_stats_direct(self, sd_uuid, dom_type, service_type):
        """
        Like get_all_host_stats(), but bypasses broker by directly accessing
        storage.
        """
        return self.get_all_stats_direct(
            sd_uuid,
            dom_type,
            service_type,
            self.StatModes.HOST)

    def _configure_broker_conn(self, broker):
        if self._config is None:
            self._config = config.Config()
        sd_uuid = self._config.get(config.ENGINE, config.SD_UUID)
        dom_type = self._config.get(config.ENGINE, config.DOMAIN_TYPE)
        broker.set_storage_domain(StorageBackendTypes.FilesystemBackend,
                                  sd_uuid=sd_uuid,
                                  dom_type=dom_type)

    def set_global_md_flag(self, flag, value):
        """
        Connects to HA broker and sets flags in global metadata, leaving
        any other flags unaltered.  On error, exceptions will be propagated
        to the caller.
        """
        try:
            transform_fn = metadata.global_flags[flag]
        except KeyError:
            raise Exception('Unknown metadata flag: {0}'.format(flag))

        # If the metadata value specifies a transformation function, send the
        # input value through it in order to normalize and/or verify the data.
        if transform_fn:
            put_val = transform_fn(value)
        else:
            put_val = value

        broker = brokerlink.BrokerLink()
        with broker.connection():
            self._configure_broker_conn(broker)
            service = constants.SERVICE_TYPE + agent_constants.MD_EXTENSION
            all_stats = broker.get_stats_from_storage(service)

            global_stats = all_stats.get(0)
            if global_stats and len(global_stats):
                try:
                    md_dict = metadata.parse_global_metadata_to_dict(
                        self._log, global_stats)
                except Exception:
                    self._log.warn("Metadata block corrupted. Correcting.")
                    md_dict = {}
            else:
                md_dict = {}

            md_dict[flag] = put_val
            block = metadata.create_global_metadata_from_dict(md_dict)
            broker.put_stats_on_storage(
                constants.SERVICE_TYPE+agent_constants.MD_EXTENSION,
                0,
                block)

    def get_local_host_id(self):
        if self._config is None:
            self._config = config.Config()
        return int(self._config.get(config.ENGINE, config.HOST_ID))

    def get_local_host_score(self):
        if self._config is None:
            self._config = config.Config()

        host_id = int(self._config.get(config.ENGINE, config.HOST_ID))
        broker = brokerlink.BrokerLink()
        with broker.connection():
            self._configure_broker_conn(broker)
            service = constants.SERVICE_TYPE + agent_constants.MD_EXTENSION
            stats = broker.get_stats_from_storage(service)

        score = 0
        if host_id in stats:
            try:
                md = metadata.parse_metadata_to_dict(host_id, stats[host_id])
            except MetadataError as e:
                self._log.error(str(e))
            else:
                # Only report a non-zero score if the local host has had a
                # recent update.
                if self._check_liveness_metadata(md, broker):
                    score = md['score']

        return score

    def set_maintenance_mode(self, mode, value):
        if mode == self.MaintenanceMode.GLOBAL:
            self.set_global_md_flag(self.GlobalMdFlags.MAINTENANCE,
                                    str(value))

        elif mode == self.MaintenanceMode.LOCAL:
            if self._config is None:
                self._config = config.Config()
            self._config.set(config.HA,
                             config.LOCAL_MAINTENANCE,
                             str(util.to_bool(value)))

        else:
            raise Exception("Invalid maintenance mode: {0}".format(mode))

    def reset_lockspace(self, force=False):
        # Lockspace file
        lockspace_file = None

        # Service names
        lockspace = (constants.SERVICE_TYPE +
                     agent_constants.LOCKSPACE_EXTENSION)
        service = (constants.SERVICE_TYPE +
                   agent_constants.MD_EXTENSION)

        # Connect to a broker and read all stats
        if self._config is None:
            self._config = config.Config()
        broker = brokerlink.BrokerLink()

        with broker.connection():
            self._configure_broker_conn(broker)
            stats = broker.get_stats_from_storage(service)
            lockspace_file = broker.get_service_path(lockspace)

        # Process raw stats
        try:
            all_stats = self._parse_stats(stats, self.StatModes.ALL)
            self._check_liveness_for_stats(all_stats, broker)
        except Exception as ex:
            self._log.exception(ex)
            all_stats = {}

        # Check whether it is safe to perform lockfile reset
        for id, stats in all_stats.iteritems():
            if id == 0:
                if (not force and
                        not stats.get(self.GlobalMdFlags.MAINTENANCE, False)):
                    raise Exception("Lockfile reset can be performed in"
                                    " global maintenance mode only.")
            else:
                if not force and not stats.get("stopped", False):
                    raise Exception("Lockfile reset cannot be performed with"
                                    " an active agent.")

        if os.path.exists(lockspace_file):
            sanlock.write_lockspace(lockspace=constants.SERVICE_TYPE,
                                    path=lockspace_file,
                                    offset=0)
