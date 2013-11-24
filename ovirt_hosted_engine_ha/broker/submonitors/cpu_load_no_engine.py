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
import time
from collections import namedtuple

from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import exceptions as exceptions
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util as util
from ovirt_hosted_engine_ha.lib import vds_client as vdsc

Ticks = namedtuple('Ticks', 'total, busy')


def register():
    return "cpu-load-no-engine"


class Submonitor(submonitor_base.SubmonitorBase):
    def setup(self, options):
        self._log = logging.getLogger("EngineHealth")
        self._log.addFilter(log_filter.IntermittentFilter())

        self._address = options.get('address')
        self._use_ssl = util.to_bool(options.get('use_ssl'))
        self._vm_uuid = options.get('vm_uuid')
        if (self._address is None
                or self._use_ssl is None
                or self._vm_uuid is None):
            raise Exception("cpu-load-no-engine requires"
                            " address, use_ssl, and vm_uuid")
        self._log.debug("address=%s, use_ssl=%r, vm_uuid=%s",
                        self._address, self._use_ssl, self._vm_uuid)

        self.engine_pid = None
        self.engine_pid_start_time = None
        self.proc_stat = None

        self.system = {'prev': None, 'cur': None}
        self.vm = {'prev': None, 'cur': None}
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
        self.update_stat_file()
        self.vm['prev'] = self.vm['cur']
        self.vm['cur'] = self.get_vm_busy_ticks()
        self.system['prev'] = self.system['cur']
        self.system['cur'] = self.get_system_ticks()
        self._log.debug("Ticks: total={0}, busy={1}, vm={2}"
                        .format(self.system['cur'].total,
                                self.system['cur'].busy,
                                self.vm['cur']))

    def get_system_ticks(self):
        with open('/proc/stat', 'r') as f:
            cpu = f.readline()
        fields = [int(x) for x in cpu.split()[1:]]
        total = sum(fields)
        busy = sum(fields[:3])
        return Ticks(total, busy)

    def get_vm_busy_ticks(self):
        if self.proc_stat is None:
            return None
        return int(self.proc_stat[13]) + int(self.proc_stat[14])

    def calculate_load(self):
        dtotal = self.system['cur'].total - self.system['prev'].total
        dbusy = self.system['cur'].busy - self.system['prev'].busy
        load = dbusy / float(dtotal)

        if self.vm['cur'] is not None and self.vm['prev'] is not None:
            dvm = self.vm['cur'] - self.vm['prev']
            # The total jiffie delta is a good-enough approximation
            engine_load = dvm / float(dtotal)
            engine_load = max(engine_load, 0.0)
        else:
            engine_load = 0.0

        load_no_engine = load - engine_load
        load_no_engine = max(load_no_engine, 0.0)

        self._log.info("System load"
                       " total={0:.4f}, engine={1:.4f}, non-engine={2:.4f}"
                       .format(load, engine_load, load_no_engine))
        self.load = load_no_engine

    def update_stat_file(self):
        if self.engine_pid:
            # Try the known pid and verify it's the same process
            fname = '/proc/{0}/stat'.format(self.engine_pid)
            try:
                with open(fname, 'r') as f:
                    self.proc_stat = f.readline().split()
            except Exception:
                self.proc_stat = None
            else:
                if int(self.proc_stat[21]) == self.engine_pid_start_time:
                    self._log.debug("VM on this host, pid %d", self.engine_pid,
                                    extra=log_filter.lf_args('vm', 60))
                else:
                    # This isn't the engine qemu process...
                    self.proc_stat = None

        if self.proc_stat is None:
            # Look for the engine vm pid and try to get the stats
            self.engine_pid = None
            self.engine_pid_start_time = None
            try:
                stats = vdsc.run_vds_client_cmd(self._address, self._use_ssl,
                                                'getVmStats', self._vm_uuid)
                pid = int(stats['statsList'][0]['pid'])
            except Exception as e:
                if isinstance(e, exceptions.DetailedError) \
                        and e.detail == "Virtual machine does not exist":
                    self._log.info("VM not on this host",
                                   extra=log_filter.lf_args('vm', 60))
                else:
                    self._log.error("Failed to getVmStats: %s", str(e),
                                    extra=log_filter.lf_args('vm', 60))
            else:
                fname = '/proc/{0}/stat'.format(pid)
                try:
                    with open(fname, 'r') as f:
                        self.proc_stat = f.readline().split()
                    self.engine_pid_start_time = int(self.proc_stat[21])
                    self.engine_pid = pid
                except Exception as e:
                    # Try again next time
                    self._log.error("Failed to read vm stats: %s", str(e),
                                    extra=log_filter.lf_args('vm', 60))
