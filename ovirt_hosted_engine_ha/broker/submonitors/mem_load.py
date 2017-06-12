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

from vdsm.client import ServerError


def register():
    return "mem-load"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("%s.MemLoad" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())

    def action(self, options):
        cli = util.connect_vdsm_json_rpc(
            logger=self._log
        )
        try:
            stats = cli.Host.getStats()
            caps = cli.Host.getCapabilities()
        except ServerError as e:
            self._log.error(e)
            self.update_result(None)
            return

        mem_size = int(caps['memSize'])
        mem_used = int(stats['memUsed'])
        mem_load = float(mem_used) / mem_size
        self._log.info("memSize: %d, memUsed: %d, Load: %f",
                       mem_size, mem_used, mem_load,
                       extra=log_filter.lf_args('status', 60))
        self.update_result(str(mem_load))
