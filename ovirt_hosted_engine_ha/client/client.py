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
import os
import time

from ..agent import constants as agent_constants
from ..env import config
from ..env import constants
from ..env import path
from ..lib import brokerlink
from ..lib import metadata
from ..lib.exceptions import MetadataError


class HAClient(object):
    def __init__(self, log=False):
        """
        Create an instance of HAClient.  If the caller has a log handler, it
        should pass in log=True, else logging will effectively be disabled.
        """
        if not log:
            logging.basicConfig(filename='/dev/null', filemode='w+',
                                level=logging.CRITICAL)
        self._log = logging.getLogger("HAClient")
        self._config = None

    def get_all_host_stats(self):
        """
        Connects to HA broker, reads stats for all hosts, and returns
        them in a dictionary as {host_id: = {key: value, ...}}
        """
        if self._config is None:
            self._config = config.Config()
        broker = brokerlink.BrokerLink()
        with broker.connection():
            stats = broker.get_stats_from_storage(
                path.get_metadata_path(self._config),
                constants.SERVICE_TYPE)

        output = {}
        for host_id, data in stats.iteritems():
            try:
                md = metadata.parse_metadata_to_dict(host_id, data)
            except MetadataError as e:
                self._log.error(str(e))
                continue
            else:
                output[host_id] = md
        return output

    def get_all_host_stats_direct(self, dom_path, service_type):
        """
        Connects to HA broker, reads stats for all hosts, and returns
        them in a dictionary as {host_id: = {key: value, ...}}
        """
        from ..broker import storage_broker

        sb = storage_broker.StorageBroker()
        path = os.path.join(dom_path, constants.SD_METADATA_DIR)
        stats = sb.get_raw_stats_for_service_type(path, service_type)

        output = {}
        for host_id, data in stats.iteritems():
            try:
                md = metadata.parse_metadata_to_dict(host_id, data)
            except MetadataError as e:
                self._log.error(str(e))
                continue
            else:
                output[host_id] = md
        return output

    def get_local_host_score(self):
        if self._config is None:
            self._config = config.Config()

        host_id = int(self._config.get(config.ENGINE, config.HOST_ID))
        broker = brokerlink.BrokerLink()
        with broker.connection():
            stats = broker.get_stats_from_storage(
                path.get_metadata_path(self._config),
                constants.SERVICE_TYPE)

        score = 0
        if host_id in stats:
            try:
                md = metadata.parse_metadata_to_dict(host_id, stats[host_id])
            except MetadataError as e:
                self._log.error(str(e))
            else:
                # Only report a non-zero score if the local host has had a
                # recent update.
                if (md['host-ts'] + agent_constants.HOST_ALIVE_TIMEOUT_SECS
                        >= time.time()):
                    score = md['score']

        return score
