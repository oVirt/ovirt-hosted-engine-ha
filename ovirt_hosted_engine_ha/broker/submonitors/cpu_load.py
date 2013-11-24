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

import subprocess

from ovirt_hosted_engine_ha.broker import submonitor_base


def register():
    return "cpu-load"


class Submonitor(submonitor_base.SubmonitorBase):
    def action(self, options):
        """
        Return the one-minute load average, normalized as a ratio of load
        to number of CPUs.
        """
        load_per_thread = 1.0
        p = subprocess.Popen(['cat', '/proc/cpuinfo'], stdout=subprocess.PIPE)
        out = p.communicate()[0]
        if p.returncode == 0:
            ncpus = len([True for c in out.split("\n")
                         if len(c.split()) and c.split()[0] == 'processor'])
            p = subprocess.Popen(['cat', '/proc/loadavg'],
                                 stdout=subprocess.PIPE)
            out = p.communicate()[0]
            if p.returncode == 0:
                load_avg = float(out.split()[0])
                load_per_thread = load_avg / ncpus
        self.update_result(load_per_thread)
