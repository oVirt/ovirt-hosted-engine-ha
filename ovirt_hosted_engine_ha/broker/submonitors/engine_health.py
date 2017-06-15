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

import json
import logging
import subprocess
import threading
from six.moves import queue

from ovirt_hosted_engine_ha.broker import constants
from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util as util
from ovirt_hosted_engine_ha.lib import engine
from ovirt_hosted_engine_ha.lib import monotonic

from vdsm.common import exception as vdsm_exception
from vdsm.client import ServerError
from vdsm.virt import vmstatus


def register():
    return "engine-health"


class Submonitor(submonitor_base.SubmonitorBase):

    def setup(self, options):
        self._log = logging.getLogger("%s.EngineHealth" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())

        self._vm_uuid = options.get('vm_uuid')
        if self._vm_uuid is None:
            raise Exception("engine-health requires vm_uuid")
        self._log.debug("vm_uuid=%s", self._vm_uuid)

        self._lock = threading.Lock()

        # Using two timestamps, because VDSM uses different method
        # to generate timestamp, with different arbitrary starting point
        self._stats_local_timestamp = None
        self._stats_vdsm_timestamp = None

        self._event_name = "virt|VM_status|%s" % str(self._vm_uuid)
        self._events_queue = queue.Queue()
        util.event_broadcaster().register_queue(self._events_queue)

        self._thread = threading.Thread(target=self._handle_events)
        self._thread.start()

    def teardown(self, options):
        # Put None to signal waiting thread to stop
        self._events_queue.put(None)
        self._thread.join()

        # Remove queue reference to unregister
        self._events_queue = None

    def action(self, options):
        # First, see if vdsm tells us it's up
        cli = util.connect_vdsm_json_rpc(
            logger=self._log
        )

        # Get timestamp before RPC call, so any future event with
        # status change will have a newer timestamp
        local_ts = monotonic.time()

        try:
            stats = cli.VM.getStats(vmID=self._vm_uuid)[0]
        except ServerError as e:
            if e.code == vdsm_exception.NoSuchVM.code:
                self._log.info("VM not on this host",
                               extra=log_filter.lf_args('status', 60))
                d = {'vm': engine.VMState.DOWN,
                     'health': engine.Health.BAD,
                     'detail': 'unknown',
                     'reason': 'vm not running on this host'}
            else:
                self._log.error(e)
                d = {'vm': 'unknown', 'health': 'unknown', 'detail': 'unknown',
                     'reason': 'failed to getVmStats'}

            with self._lock:
                self._stats_local_timestamp = local_ts
                self._stats_vdsm_timestamp = None
                self.update_result(json.dumps(d))

            return

        # Convert timestamp to string in case it is an int
        vdsm_ts = str(stats.get("statusTime"))
        self._update_stats(stats, vdsm_ts, local_ts)

    def _update_stats(self, stats, vdsm_ts, local_ts):
        with self._lock:
            # Only update newer stats

            # Compare by VDSM timestamp if it exists
            if vdsm_ts and self._stats_vdsm_timestamp:
                is_newer = vdsm_ts > self._stats_vdsm_timestamp
            else:
                is_newer = local_ts > self._stats_local_timestamp

            if not is_newer:
                self._log.debug(
                    "Update has older timestamp: "
                    "(vdsm: %s, local: %s) < (vdsm: %s, local: %s)",
                    vdsm_ts,
                    local_ts,
                    self._stats_vdsm_timestamp,
                    self._stats_local_timestamp
                )
                return

            self._stats_vdsm_timestamp = vdsm_ts
            self._stats_local_timestamp = local_ts

            res = self._result_from_stats(stats)
            self._log.debug("Result set to: %s", res)
            self.update_result(json.dumps(res))

    def _result_from_stats(self, stats):
        vm_status = stats['status']

        # Report states that are not really Up, but should be
        # reported as such
        if vm_status in (vmstatus.PAUSED,
                         vmstatus.WAIT_FOR_LAUNCH,
                         vmstatus.RESTORING_STATE,
                         vmstatus.POWERING_UP):
            self._log.info("VM status: %s", vm_status,
                           extra=log_filter.lf_args('status', 60))
            return {'vm': engine.VMState.UP,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'bad vm status'}

        # Check if another host was faster in acquiring the storage lock
        exit_message = stats.get('exitMessage', "")
        if vm_status == vmstatus.DOWN and (
            exit_message.endswith('Failed to acquire lock: error -243') or
            exit_message.endswith(
                'Failed to acquire lock: Lease is held by another host'
            )
        ):
            return {'vm': engine.VMState.ALREADY_LOCKED,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'Storage of VM is locked. '
                              'Is another host already starting the VM?'}

        # Check for states that are definitely down
        if vm_status in (vmstatus.DOWN, vmstatus.MIGRATION_DESTINATION):
            self._log.info("VM not running on this host, status %s", vm_status,
                           extra=log_filter.lf_args('status', 60))
            return {'vm': engine.VMState.DOWN,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'bad vm status'}

        # VM is probably up, let's see if engine is up by polling
        # health status page
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY,
                              '--check-liveliness'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        if p.returncode != 0:
            self._log.warning("bad health status: %s", output[0])
            return {'vm': engine.VMState.UP,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'failed liveliness check'}

        self._log.info("VM is up on this host with healthy engine",
                       extra=log_filter.lf_args('status', 60))
        return {'vm': engine.VMState.UP,
                'health': engine.Health.GOOD,
                'detail': vm_status}

        # FIXME remote db down status

    def _handle_events(self):
        while True:
            ev = self._events_queue.get()
            if ev is None:
                break

            name, params = ev

            if ((self._event_name not in name) or
                    (str(self._vm_uuid) not in params)):
                continue

            self._log.debug("Received event. Name: %s, Params: %s",
                            name,
                            params)

            # Convert timestamp to string in case it is an int
            vdsm_ts = str(params["notify_time"])

            local_ts = monotonic.time()
            stats = params[str(self._vm_uuid)]
            self._update_stats(stats, vdsm_ts, local_ts)
