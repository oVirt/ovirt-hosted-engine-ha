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
import re
import subprocess
import threading
import queue

from ovirt_hosted_engine_ha.broker import constants
from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util as util
from ovirt_hosted_engine_ha.lib import engine
from ovirt_hosted_engine_ha.lib import monotonic

from vdsm.common import exception as vdsm_exception
from vdsm.client import ServerError
from vdsm.virt import vmstatus
from vdsm.virt import vmexitreason


def register():
    return "engine-health"


FAILED_TO_ACQUIRE_LOCK_PATTERN = re.compile(
    r"Is another process using the image \[.*\]\?$"
)


class Submonitor(submonitor_base.SubmonitorBase):

    def setup(self, options):
        self._log = logging.getLogger("%s.EngineHealth" % __name__)
        self._log.addFilter(log_filter.get_intermittent_filter())

        self._vm_uuid = options.get('vm_uuid')
        if self._vm_uuid is None:
            raise Exception("engine-health requires vm_uuid")
        self._log.debug("vm_uuid=%s", self._vm_uuid)

        # Most recent VM state
        self._vm_state = engine.VMState.DOWN

        self._lock = threading.Lock()

        # Using two timestamps, because VDSM uses different method
        # to generate timestamp, with different arbitrary starting point
        self._stats_local_timestamp = 0
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

                if self._vm_state == engine.VMState.UP:
                    self._vm_state = engine.VMState.DOWN_MISSING

                d = {'vm': self._vm_state,
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
            if not self._newer_timestamp(vdsm_ts, local_ts):
                self._log.debug(
                    "Update has older timestamp: "
                    "(vdsm: %s, local: %s) < (vdsm: %s, local: %s)",
                    vdsm_ts,
                    local_ts,
                    self._stats_vdsm_timestamp,
                    self._stats_local_timestamp
                )
                return

        # Getting result can take a long time, because
        # it communicates with the engine
        res = self._result_from_stats(stats)

        with self._lock:
            if not self._newer_timestamp(vdsm_ts, local_ts):
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

            self._log.debug("Result set to: %s", res)
            self.update_result(json.dumps(res))

    def _newer_timestamp(self, vdsm_ts, local_ts):
        # Compare by VDSM timestamp if it exists
        if vdsm_ts and self._stats_vdsm_timestamp:
            return vdsm_ts > self._stats_vdsm_timestamp

        return local_ts > self._stats_local_timestamp

    def _result_from_stats(self, stats):
        vm_status = stats['status']

        # Check if another host was faster in acquiring the storage lock
        exit_message = stats.get('exitMessage', "")
        if (
            vm_status == vmstatus.DOWN and
            self._failed_to_acquire_lock(exit_message)
        ):
            self._log.info(
                "VM storage is already locked.",
                extra=log_filter.lf_args('status', 60)
            )
            self._vm_state = engine.VMState.DOWN
            return {'vm': self._vm_state,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'Storage of VM is locked. '
                              'Is another host already starting the VM?'}

        # Check if VM migration succeeded
        if (
            vm_status == vmstatus.DOWN and
            stats.get('exitReason', 0) == vmexitreason.MIGRATION_SUCCEEDED
        ):
            self._log.info(
                "VM successfully migrated away from this host.",
                extra=log_filter.lf_args('status', 60)
            )
            self._vm_state = engine.VMState.DOWN
            return {'vm': self._vm_state,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'VM migrated away successfully'}

        # Check for states that are definitely down
        if vm_status in (vmstatus.DOWN, vmstatus.MIGRATION_DESTINATION):
            self._log.info("VM not running on this host, status %s", vm_status,
                           extra=log_filter.lf_args('status', 60))

            if self._vm_state != engine.VMState.DOWN:
                self._vm_state = engine.VMState.DOWN_UNEXPECTED

            return {'vm': self._vm_state,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'bad vm status'}

        # Report states that are not really Up, but should be
        # reported as such
        if vm_status in (vmstatus.PAUSED,
                         vmstatus.WAIT_FOR_LAUNCH,
                         vmstatus.RESTORING_STATE,
                         vmstatus.POWERING_UP):
            self._log.info("VM status: %s", vm_status,
                           extra=log_filter.lf_args('status', 60))
            self._vm_state = engine.VMState.UP
            return {'vm': self._vm_state,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'bad vm status'}

        # VM is probably up, let's see if engine is up by polling
        # health status page
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY,
                              '--check-liveliness'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        stdout = output[0].decode()
        if p.returncode != 0:
            self._log.warning("bad health status: %s", stdout)
            self._vm_state = engine.VMState.UP
            return {'vm': self._vm_state,
                    'health': engine.Health.BAD,
                    'detail': vm_status,
                    'reason': 'failed liveliness check'}

        self._log.info("VM is up on this host with healthy engine",
                       extra=log_filter.lf_args('status', 60))
        self._vm_state = engine.VMState.UP
        return {'vm': self._vm_state,
                'health': engine.Health.GOOD,
                'detail': vm_status}

        # FIXME remote db down status

    def _failed_to_acquire_lock(self, exit_message):
        return (
            exit_message.endswith('Failed to acquire lock: error -243') or
            exit_message.endswith(
                'Failed to acquire lock: Lease is held by another host') or
            exit_message.endswith('Is another process using the image?') or
            FAILED_TO_ACQUIRE_LOCK_PATTERN.search(exit_message) is not None
        )

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
