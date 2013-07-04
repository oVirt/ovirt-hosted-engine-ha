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
import subprocess

from ovirt_hosted_engine_ha.broker import constants
from ovirt_hosted_engine_ha.broker import submonitor_base


def register():
    return "engine-health"


class Submonitor(submonitor_base.SubmonitorBase):
    def action(self, options):
        # Rely on hosted-engine for status
        log = logging.getLogger("EngineHealth")

        # FIXME use this when `hosted-engine --vm-status` is implemented
        """
        # First see if VM is holding a lock on its storage...
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY, '--vm-status'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        if p.returncode != 0:
            log.warning("Engine VM not running: %s", output[0])
            self.update_result("down")
            return

        # VM is up, see if the engine inside it is healthy
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY,
                              '--check-liveliness'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        if p.returncode != 0:
            log.warning("Engine VM up but bad health status: %s", output[0])
            self.update_result("up bad-health-status")
            return
        else:
            self.update_result("up good-health-status")
        """
        # For now, just look at the health status page
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY,
                              '--check-liveliness'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        if p.returncode != 0:
            log.warning("bad health status: %s", output[0])
            self.update_result("down")
            return
        else:
            self.update_result("up good-health-status")
