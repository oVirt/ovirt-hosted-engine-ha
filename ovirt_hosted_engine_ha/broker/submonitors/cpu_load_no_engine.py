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
import multiprocessing
import time
from collections import namedtuple

from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util as util

from vdsm.common import exception as vdsm_exception
from vdsm.client import ServerError


Ticks = namedtuple('Ticks', 'total, busy')


def register():
    return "cpu-load-no-engine"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("%s.CpuLoadNoEngine" % __name__)
        self._log.addFilter(log_filter.get_intermittent_filter())

        self._vm_uuid = options.get('vm_uuid')
        if self._vm_uuid is None:
            raise Exception("cpu-load-no-engine requires vm_uuid")
        self._log.debug("vm_uuid=%s", self._vm_uuid)

        self.system = {'prev': None, 'cur': None}
        self.latest_report_ts = None
        self.load = 0.0

    def action(self, options):
        """
        Return the one-minute load average, normalized as a ratio of load to
        number of CPUs, and without the impact of load from the engine VM.
        """
        if self.latest_report_ts is None:
            # For first reading, take 10-second average
            self.refresh_ticks()
            time.sleep(10)
        elif not util.has_elapsed(self.latest_report_ts, 60):
            return self.load

        self.refresh_ticks()
        self.calculate_load()
        self.update_result("{0:.4f}".format(self.load))
        self.latest_report_ts = time.time()

    def refresh_ticks(self):
        self.system['prev'] = self.system['cur']
        self.system['cur'] = self.get_system_ticks()
        self._log.debug("Ticks: total={0}, busy={1}"
                        .format(self.system['cur'].total,
                                self.system['cur'].busy))

    def get_system_ticks(self):
        with open('/proc/stat', 'r') as f:
            cpu = f.readline()
        fields = [int(x) for x in cpu.split()[1:]]

        # Ignore the last fields, 'guest' and 'guest_nice',
        # because they are already counted in 'user' and 'nice' time
        total = sum(fields[:8])

        idle = sum(fields[3:5])
        busy = total - idle

        return Ticks(total, busy)

    def calculate_load(self):
        dtotal = self.system['cur'].total - self.system['prev'].total
        dbusy = self.system['cur'].busy - self.system['prev'].busy
        load = dbusy / float(dtotal)

        cli = util.connect_vdsm_json_rpc(
            logger=self._log
        )

        engine_load = 0.0
        try:
            stats = cli.VM.getStats(vmID=self._vm_uuid)[0]
            vm_cpu_total = float(stats["cpuUser"]) + float(stats["cpuSys"])
            cpu_count = multiprocessing.cpu_count()
            engine_load = (vm_cpu_total / cpu_count) / 100.0
        except ServerError as e:
            if e.code == vdsm_exception.NoSuchVM.code:
                self._log.info("VM not on this host",
                               extra=log_filter.lf_args('vm', 60))
            else:
                self._log.error(e, extra=log_filter.lf_args('vm', 60))
        except KeyError:
            self._log.info(
                "VM stats do not contain cpu usage. VM might be down.",
                extra=log_filter.lf_args('vm', 60)
            )
        except ValueError as e:
            self._log.error("Error getting cpuUser: %s", str(e))

        load_no_engine = load - engine_load
        load_no_engine = max(load_no_engine, 0.0)

        self._log.info("System load"
                       " total={0:.4f}, engine={1:.4f}, non-engine={2:.4f}"
                       .format(load, engine_load, load_no_engine))
        self.load = load_no_engine
