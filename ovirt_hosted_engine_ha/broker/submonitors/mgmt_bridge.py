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

from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util as util
from ovirt_hosted_engine_ha.lib import vds_client as vdsc


def register():
    return "mgmt-bridge"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("MgmtBridge")
        self._log.addFilter(log_filter.IntermittentFilter())
        self._address = options.get('address')
        self._use_ssl = util.to_bool(options.get('use_ssl'))
        self._bridge = options.get('bridge_name')
        if (self._address is None or self._use_ssl is None
                or self._bridge is None):
            raise Exception("mgmt-bridge requires address, use_ssl flag,"
                            " and bridge name")
        self._log.debug("address=%s, use_ssl=%r, bridge=%s",
                        self._address, self._use_ssl, self._bridge)

    def action(self, options):
        try:
            response = vdsc.run_vds_client_cmd(self._address, self._use_ssl,
                                               'getVdsCapabilities')
        except Exception as e:
            self._log.error("Failed to getVdsCapabilities: %s", str(e))
            self.update_result(None)
            return

        if ('bridges' in response['info']
                and self._bridge in response['info']['bridges']):
            # FIXME check status of bridge?
            self._log.info("Found bridge %s", self._bridge,
                           extra=log_filter.lf_args('status', 60))
            self.update_result(True)
        else:
            self._log.info("Bridge %s not found", self._bridge,
                           extra=log_filter.lf_args('status', 60))
            self.update_result(False)
