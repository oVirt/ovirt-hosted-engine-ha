#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2017 Red Hat, Inc.
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

from ovirt_hosted_engine_ha.broker import constants
from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util as util

from vdsm.client import ServerError


def register():
    return "storage-domain"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("%s.StorageDomain" % __name__)
        self._log.addFilter(log_filter.get_intermittent_filter())

        self.sd_uuid = options["sd_uuid"]

    def action(self, options):
        try:
            cli = util.connect_vdsm_json_rpc(
                logger=self._log
            )

            status = cli.Host.getStorageRepoStats(domains=[self.sd_uuid])
        except ServerError as e:
            self._log.error(str(e))
            self.update_result(False)
            return

        try:
            valid = status[self.sd_uuid]['valid']
            delay = float(status[self.sd_uuid]['delay'])
            if valid and delay <= constants.STORAGE_DELAY:
                self.update_result(True)
                return
        except Exception:
            self._log.warn("Hosted-engine storage domain is in invalid state")
        self.update_result(False)
