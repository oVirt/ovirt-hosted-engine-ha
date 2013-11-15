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
import subprocess

from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter


def register():
    return "ping"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("Ping")
        self._log.addFilter(log_filter.IntermittentFilter())
        self._addr = options.get('addr')
        self._timeout = str(options.get('timeout', 10))
        if self._addr is None:
            raise Exception("ping requires addr address")
        self._log.debug("addr=%s, timeout=%s", self._addr, self._timeout)

    def action(self, options):
        with open(os.devnull, "w") as devnull:
            p = subprocess.Popen(['ping', '-c', '1', '-W',
                                  self._timeout, self._addr],
                                 stdout=devnull, stderr=devnull)
            if p.wait() != 0:
                self._log.warning("Failed to ping %s", self._addr)
                self.update_result(False)
            else:
                self._log.info("Successfully pinged %s", self._addr,
                               extra=log_filter.lf_args('status', 60))
                self.update_result(True)
