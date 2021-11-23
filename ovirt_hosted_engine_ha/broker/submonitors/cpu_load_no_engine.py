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
        self.latest_real_stats_ts = None
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
        cpu_data_is_real = False
        vm_on_this_host = False
        try:
            stats = cli.VM.getStats(vmID=self._vm_uuid)[0]
            vm_on_this_host = True
            vm_cpu_total = float(stats["cpuUser"]) + float(stats["cpuSys"])
            cpu_count = multiprocessing.cpu_count()
            engine_load = (vm_cpu_total / cpu_count) / 100.0
            # This is a hack. vdsm initializes cpuUsage to 0.00, and when it
            # gets a result from libvirt (as 'cpu.user', 'cpu.system'), sets
            # it to libvirt's value. cpuUser and cpuSystem are also initialized
            # to '0.00', but can also have '0.00' as a legit value afterwards.
            # But cpuUsage, if it has a value from libvirt, is always an
            # integer. Actually, AFAICT, initializing it to '0.00' might be
            # considered a bug. Anyway, rely on this for deciding whether
            # cpuUser/cpuSystem are real or init values.
            # TODO: Extend VDSM's API to include this information explicitly,
            # e.g. by adding a new field, say 'stats_from_libvirt' which is
            # True or False, and base the decision on this.
            cpu_data_is_real = stats['cpuUsage'] != '0.00'
        except ServerError as e:
            if e.code == vdsm_exception.NoSuchVM.code:
                self._log.info("VM not on this host",
                               extra=log_filter.lf_args('vm', 60))
                self.latest_real_stats_ts = None
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

        if cpu_data_is_real or not vm_on_this_host:
            self._log.info("System load"
                           " total={0:.4f}, engine={1:.4f}, non-engine={2:.4f}"
                           .format(load, engine_load, load_no_engine))
            self.load = load_no_engine
            self.latest_real_stats_ts = time.time()
        else:
            # In certain cases, we got cpuUser=0.00 for up to around
            # 90 seconds after a VM was up, causing what seems like
            # a "general" high cpu load unrelated to that VM.
            # This caused problems with hosted-engine HA daemons,
            # which lower the score of that host due to that load.
            # Rely on cpuUsage value instead. See also:
            # https://lists.ovirt.org/archives/list/devel@ovirt.org/thread/\
            # 7HNIFCW4NENG4ADZ5ROT43TCDXDURRJB/
            if self.latest_real_stats_ts is None:
                # Just ignore, but start counting
                self.latest_real_stats_ts = time.time()
            elif not util.has_elapsed(self.latest_real_stats_ts, 300):
                self._log.info("Ignoring cpuUser/cpuSys, init values")
            else:
                # No real data, and for more than 5 minutes.
                # It's probably bad enough that we should just
                # not ignore - so if cpu load is high, just report
                # that, and if as a result the score will be low
                # and the VM will be shut down - so be it.
                self._log.info(
                    "System load"
                    " total={0:.4f}, engine={1:.4f}, non-engine={2:.4f}"
                    .format(load, engine_load, load_no_engine))
                self._log.info("engine VM cpu usage is not up-to-date")
                self.load = load_no_engine
                # Do not update self.latest_real_stats_ts, keep counting
