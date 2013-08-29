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
        self._config = config.Config()

    def get_all_host_stats(self):
        """
        Connects to HA broker, reads stats for all hosts, and returns
        them in a dictionary as {host_id: = {key: value, ...}}
        """
        broker = brokerlink.BrokerLink()
        broker.connect()
        stats = broker.get_stats_from_storage(
            path.get_metadata_path(self._config),
            constants.SERVICE_TYPE)
        broker.disconnect()

        output = {}
        for host_str, data in stats.iteritems():
            try:
                md = metadata.parse_metadata_to_dict(host_str, data)
            except MetadataError as e:
                self._log.error(str(e))
                continue
            else:
                output[md['host-id']] = md
        return output
