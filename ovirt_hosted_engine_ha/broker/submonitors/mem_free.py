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
    return "mem-free"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("%s.MemFree" % __name__)
        self._log.addFilter(log_filter.get_intermittent_filter())

    def action(self, options):
        cli = util.connect_vdsm_json_rpc(
            logger=self._log
        )
        try:
            stats = cli.Host.getStats()
        except ServerError as e:
            self._log.error(e)
            self.update_result(None)
            return

        mem_free = str(stats['memFree'])
        self._log.info("memFree: %s", mem_free,
                       extra=log_filter.lf_args('status', 60))
        self.update_result(mem_free)
