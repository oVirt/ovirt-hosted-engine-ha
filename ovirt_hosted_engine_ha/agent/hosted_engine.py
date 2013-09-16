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

import errno
import logging
import os
import socket
import subprocess
import time

import sanlock

from . import constants
from ..env import config
from ..lib import brokerlink
from ..lib import exceptions as ex
from ..lib import log_filter
from ..lib import metadata
from ..lib import util
from ..lib import vds_client as vdsc


def handler_cleanup(f):
    """
    Call a cleanup function when transitioning out of a state
    (i.e. when the handler returns a state other than its own)
    """
    def cleanup_wrapper(self):
        ret = f(self)
        if ret[0] != self._rinfo['current-state']:
            cleanup_fn = f.__name__ + '_cleanup'
            getattr(self, cleanup_fn)()
        return ret
    return cleanup_wrapper


class HostedEngine(object):
    LF_MD_ERROR = 'LF_MD_ERROR'
    LF_HOST_UPDATE = 'LF_HOST_UPDATE'
    LF_HOST_UPDATE_DETAIL = 'LF_HOST_UPDATE_DETAIL'
    LF_ENGINE_HEALTH = 'LF_ENGINE_HEALTH'

    MIGRATION_THRESHOLD_SCORE = 800

    engine_status_score_lookup = {
        'None': 0,
        'vm-down': 1,
        'vm-up bad-health-status': 2,
        'vm-up good-health-status': 3,
    }

    class States(object):
        ENTRY = 'ENTRY'
        OFF = 'OFF'
        START = 'START'
        ON = 'ON'
        STOP = 'STOP'
        MIGRATE = 'MIGRATE'

    class MigrationStatus(object):
        PENDING = 'PENDING'
        STARTED = 'STARTED'
        IN_PROGRESS = 'IN_PROGRESS'
        DONE = 'DONE'
        FAILURE = 'FAILURE'

    def __init__(self, shutdown_requested_callback):
        """
        Initialize hosted engine monitoring logic.  shutdown_requested_callback
        is a callback returning True/False depending on whether ha agent
        shutdown has been requested.
        """
        self._log = logging.getLogger("HostedEngine")
        self._log.addFilter(log_filter.IntermittentFilter())

        self._shutdown_requested_callback = shutdown_requested_callback
        self._config = config.Config()

        self._broker = None
        self._required_monitors = self._get_required_monitors()
        self._local_monitors = {}
        self._rinfo = {}
        self._init_runtime_info()
        self._all_host_stats = {}

        self._sd_path = None
        self._metadata_path = None

        self._sanlock_initialized = False

        self._vm_state_actions = {
            self.States.ENTRY: self._handle_entry,
            self.States.OFF: self._handle_off,
            self.States.START: self._handle_start,
            self.States.ON: self._handle_on,
            self.States.STOP: self._handle_stop,
            self.States.MIGRATE: self._handle_migrate,
        }

    def _get_required_monitors(self):
        """
        Called by __init__(), see self._required_monitors

        For each entry:
         'field' - field name in the _local_monitors dict, holding details:
                    'id' - id of started submonitor (or None if not started)
                    'status' - last status returned by this monitor
         'monitor' - monitor type, e.g. ping or cpu-load
         'options' - dict of options needed by this monitor
        """
        req = []
        req.append({
            'field': 'gateway',
            'monitor': 'ping',
            'options': {
                'addr': self._config.get(config.ENGINE, config.GATEWAY_ADDR)}
        })
        req.append({
            'field': 'bridge',
            'monitor': 'mgmt-bridge',
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, config.VDSM_SSL),
                'bridge_name': self._config.get(
                    config.ENGINE, config.BRIDGE_NAME
                )}
        })
        req.append({
            'field': 'mem-free',
            'monitor': 'mem-free',
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, config.VDSM_SSL)}
        })
        req.append({
            'field': 'cpu-load',
            'monitor': 'cpu-load',
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, config.VDSM_SSL)}
        })
        req.append({
            'field': 'mem-load',
            'monitor': 'mem-load',
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, config.VDSM_SSL)}
        })
        req.append({
            'field': 'engine-health',
            'monitor': 'engine-health',
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, config.VDSM_SSL),
                'vm_uuid': self._config.get(config.VM, config.VM_UUID)}
        })
        return req

    def _init_runtime_info(self):
        """
        Initialize self._rinfo dict (and document the entries).
        """
        # Local timestamp of most recent engine vm startup attempt
        self._rinfo['engine-vm-retry-time'] = None

        # Count of recent engine vm startup attempts
        self._rinfo['engine-vm-retry-count'] = 0

        # Local timestamp when health status caused vm shutdown
        self._rinfo['bad-health-failure-time'] = None

        # Host id of local host
        self._rinfo['host-id'] = int(self._config.get(config.ENGINE,
                                                      config.HOST_ID))

        # Initial state to track engine vm status in state machine
        self._rinfo['current-state'] = self.States.ENTRY

        # The following are initialized when needed to process engine actions

        # Used to denote best-ranked engine status of all live hosts
        # 'best-engine-status'
        # 'best-engine-status-host-id'

        # Highest score of all hosts, and host-id with that score
        # 'best-score'
        # 'best-score-host-id'

        # Current state machine state, member of self.States
        # 'current-state'

        # State of maintenance bit, True/False
        # 'maintenance'

        # Used by ON state; tracks time when bad status first seen, cleanred
        # on either state change due to healthy state or timeout
        # 'first-bad-status-time'

        # Used by ON and MIGRATE state, tracks status of migration (element of
        # self.MigrationStatus) and host id to which migration is occurring
        # 'migration-host-id'
        # 'migration-status'

    def _get_lf_args(self, lf_class):
        return {'lf_class': lf_class,
                'interval': constants.INTERMITTENT_LOG_INTERVAL_SECS}

    def start_monitoring(self):
        error_count = 0

        while not self._shutdown_requested_callback():
            try:
                self._initialize_broker()
                self._initialize_vdsm()
                self._initialize_sanlock()

                self._collect_local_stats()
                blocks = self._generate_local_blocks()
                self._push_to_storage(blocks)

                self._collect_all_host_stats()
                self._perform_engine_actions()

            except Exception as e:
                self._log.warning("Error while monitoring engine: %s", str(e))
                if not (isinstance(e, ex.DisconnectionError) or
                        isinstance(e, ex.RequestError)):
                    self._log.warning("Unexpected error", exc_info=True)

                delay = 60
                error_count += 1
                log_level = logging.INFO

            else:
                delay = 10
                error_count = 0  # All is well, reset the error counter
                log_level = logging.DEBUG

            if error_count >= constants.MAX_ERROR_COUNT:
                self._log.error("Shutting down the agent because of "
                                "%d failures in a row!",
                                constants.MAX_ERROR_COUNT)
                break

            self._log.log(log_level, "Sleeping %d seconds", delay)
            time.sleep(delay)

        self._log.debug("Disconnecting from ha-broker")
        if self._broker and self._broker.is_connected():
            self._broker.disconnect()

    def _initialize_broker(self):
        if self._broker and self._broker.is_connected():
            return
        self._log.info("Initializing ha-broker connection")
        if not self._broker:
            self._broker = brokerlink.BrokerLink()
        try:
            self._broker.connect()
        except Exception as e:
            self._log.error("Failed to connect to ha-broker: %s", str(e))
            raise

        for m in self._required_monitors:
            try:
                lm = {}
                lm['id'] = self._broker.start_monitor(m['monitor'],
                                                      m.get('options', {}))
                lm['status'] = self._broker.get_monitor_status(lm['id'])
            except ex.RequestError:
                self._log.error("Failed to start necessary monitors")
                # Stopping monitors will occur automatically upon disconnection
                self._broker.disconnect()
                self._broker = None
                raise
            else:
                self._local_monitors[m['field']] = lm
        self._log.info("Broker initialized, all submonitors started")

    def _initialize_vdsm(self):
        # TODO not the most efficient means to maintain vdsmd...
        self._cond_start_service('vdsmd')

        self._log.debug("Verifying storage is attached")
        tries = 0
        while tries < constants.MAX_VDSM_WAIT_SECS:
            tries += 1
            # `hosted-engine --connect-storage` internally calls vdsClient's
            # connectStorageServer command, which can be executed repeatedly
            # without issue even if the storage is already connected.  Note
            # that if vdsm was just started, it might take a few seconds to
            # initialize before accepting commands, thus the retries.
            cmd = [constants.HOSTED_ENGINE_BINARY, '--connect-storage']
            self._log.debug("Executing {0}".format(cmd))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            output = p.communicate()
            self._log.debug("Attempt %d, return code: %d", tries, p.returncode)
            self._log.debug("stdout: %s", output[0])
            self._log.debug("stderr: %s", output[1])
            if p.returncode == 0:
                self._log.info("Successfully verified that VDSM"
                               " is attached to storage")
                break
        if tries == constants.MAX_VDSM_WAIT_SECS:
            self._log.error("Failed trying to connect storage: %s", output[1])
            raise Exception("Failed trying to connect storage")

        # Update to the current mount path for the domain
        self._sd_path = self._get_domain_path()
        self._log.debug("Path to storage domain is %s", self._sd_path)

    def _cond_start_service(self, service_name):
        self._log.debug("Checking %s status", service_name)
        with open(os.devnull, "w") as devnull:
            p = subprocess.Popen(['sudo',
                                  'service', service_name, 'status'],
                                 stdout=devnull, stderr=devnull)
            if p.wait() == 0:
                self._log.info("%s running", service_name)
            else:
                self._log.error("Starting %s", service_name)
                with open(os.devnull, "w") as devnull:
                    p = subprocess.Popen(['sudo',
                                          'service', service_name, 'start'],
                                         stdout=devnull,
                                         stderr=subprocess.PIPE)
                    res = p.communicate()
                if p.returncode != 0:
                    raise Exception("Could not start {0}: {1}"
                                    .format(service_name, res[1]))

    def _get_domain_path(self):
        """
        Return path of storage domain holding engine vm
        """
        sd_uuid = self._config.get(config.ENGINE, config.SD_UUID)
        parent = constants.SD_MOUNT_PARENT
        for dname in os.listdir(parent):
            path = os.path.join(parent, dname, sd_uuid)
            if os.access(path, os.F_OK):
                return path
        raise Exception("path to storage domain {0} not found in {1}"
                        .format(sd_uuid, parent))

    def _initialize_sanlock(self):
        self._cond_start_service('sanlock')

        host_id = self._rinfo['host-id']
        self._metadata_dir = os.path.join(self._sd_path,
                                          constants.SD_METADATA_DIR)
        lease_file = os.path.join(self._metadata_dir,
                                  constants.SERVICE_TYPE + '.lockspace')
        if not self._sanlock_initialized:
            lvl = logging.INFO
        else:
            lvl = logging.DEBUG
        self._log.log(lvl, "Ensuring lease for lockspace %s, host id %d"
                           " is acquired (file: %s)",
                           constants.LOCKSPACE_NAME, host_id, lease_file)

        try:
            sanlock.add_lockspace(constants.LOCKSPACE_NAME,
                                  host_id, lease_file)
        except sanlock.SanlockException as e:
            acquired_lock = False
            msg = None
            if hasattr(e, 'errno'):
                if e.errno == errno.EEXIST:
                    self._log.debug("Host already holds lock")
                    acquired_lock = True
                elif e.errno == errno.EINVAL:
                    msg = ("cannot get lock on host id {0}:"
                           " host already holds lock on a different host id"
                           .format(host_id))
                elif e.errno == errno.EINTR:
                    msg = ("cannot get lock on host id {0}:"
                           " sanlock operation interrupted (will retry)"
                           .format(host_id))
                elif e.errno == errno.EINPROGRESS:
                    msg = ("cannot get lock on host id {0}:"
                           " sanlock operation in progress (will retry)"
                           .format(host_id))
            if not acquired_lock:
                if not msg:
                    msg = ("cannot get lock on host id {0}: {1}"
                           .format(host_id, str(e)))
                self._log.error(msg, exc_info=True)
                raise Exception("Failed to initialize sanlock: {0}"
                                .format(msg))
        else:
            self._log.info("Acquired lock on host id %d", host_id)
        self._sanlock_initialized = True

    def _collect_local_stats(self):
        """
        Refresh all required submonitors and update local state.
        """
        self._log.debug("Refreshing all submonitors")
        for k, v in self._local_monitors.iteritems():
            self._local_monitors[k]['status'] = (
                self._broker.get_monitor_status(v['id']))
        self._log.debug("Refresh complete")

        # re-initialize retry status variables if the retry window
        # has expired.
        if (self._rinfo['engine-vm-retry-time'] is not None
            and self._rinfo['engine-vm-retry-time']
                < time.time() - constants.ENGINE_RETRY_EXPIRATION_SECS):
            self._rinfo['engine-vm-retry-time'] = None
            self._rinfo['engine-vm-retry-count'] = 0
            self._log.debug("Cleared retry status")

        # reset health status variable after expiration
        # FIXME it would be better to time this based on # of hosts available
        # to run the vm, not just a one-size-fits-all timeout
        if (self._rinfo['bad-health-failure-time'] is not None
                and self._rinfo['bad-health-failure-time']
                < time.time() - constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS):
            self._rinfo['bad-health-failure-time'] = None
            self._log.debug("Cleared bad health status")

    def _generate_local_blocks(self):
        """
        Calculates the host score from local monitor info and places the
        score on shared storage in the following format:

          {md_parse_vers}|{md_feature_vers}|{ts_int}
            |{host_id}|{score}|{engine_status}|{name}

        The compiled score is later read back from the storage, parsed from
        the string above, and used to decide where engine should run (host
        with the highest score wins).

        The score is  based on a variety of factors each having different
        weights; they are scaled such that minor factors are not considered
        at all unless major factors are equal.  For example, a host with an
        unreachable gateway will never be chosen over one with the gateway
        up due to extra cpu/memory being available.

        Additional adjustments are made for the retry count of starting the
        engine VM.  If the VM can't be started, an equally-suitable host
        should be given the next chance.  After a few (ENGINE_RETRY_COUNT)
        failed attempts, the host's score is set to 0 to give any lesser-
        suited hosts a chance.  After ENGINE_RETRY_EXPIRATION_SECS seconds,
        this host's retry count will no longer be factored into the score.
        If retries are still occurring amonst the HA hosts at that time, this
        host will again have an opportunity to run the engine VM.

        Score weights:
        1000 - gateway address is pingable
         800 - host's management network bridge is up
         400 - host has 4GB of memory free to run the engine VM
         100 - host's cpu load is less than 80% of capacity
         100 - host's memory usage is less than 80% of capacity

        Adjustments:
         -50 - subtraction for each failed start-vm retry attempt
           0 - score reset to 0 after ENGINE_RETRY_COUNT attempts,
               until ENGINE_RETRY_EXPIRATION_SECS seconds have passed
        """

        def float_or_default(value, default):
            try:
                return float(value)
            except ValueError:
                return default

        lm = self._local_monitors

        score = 0
        # FIXME score needed for vdsm storage pool connection?
        # (depending on storage integration, may not be able to report...)
        score += 1000 * (1 if lm['gateway']['status'] == 'True' else 0)
        score += 800 * (1 if lm['bridge']['status'] == 'True' else 0)
        score += 400 * (1 if float_or_default(lm['mem-free']['status'], 0)
                        >= 4096.0 else 0)
        score += 100 * (1 if float_or_default(lm['cpu-load']['status'], 1)
                        < 0.8 else 0)
        score += 100 * (1 if float_or_default(lm['mem-load']['status'], 1)
                        < 0.8 else 0)

        # Subtracting a small amount each time causes round-robin attempts
        # between hosts that are otherwise equally suited to run the engine
        score -= 50 * self._rinfo['engine-vm-retry-count']
        score = max(0, score)

        # If too many retries occur, give a less-suited host a chance
        if (self._rinfo['engine-vm-retry-count']
                > constants.ENGINE_RETRY_COUNT):
            score = 0

        # If engine has bad health status, let another host try
        if self._rinfo['bad-health-failure-time']:
            score = 0

        ts = int(time.time())
        data = ("{md_parse_vers}|{md_feature_vers}|{ts_int}"
                "|{host_id}|{score}|{engine_status}|{name}"
                .format(md_parse_vers=constants.METADATA_PARSE_VERSION,
                        md_feature_vers=constants.METADATA_FEATURE_VERSION,
                        ts_int=ts,
                        host_id=self._rinfo['host-id'],
                        score=score,
                        engine_status=lm['engine-health']['status'],
                        name=socket.gethostname()))
        if len(data) > constants.METADATA_BLOCK_BYTES:
            raise Exception("Output metadata too long ({0} bytes)"
                            .format(data))

        info = ("metadata_parse_version={md_parse_vers}\n"
                "metadata_feature_version={md_feature_vers}\n"
                "timestamp={ts_int} ({ts_str})\n"
                "host-id={host_id}\n"
                "score={score}\n"
                .format(md_parse_vers=constants.METADATA_PARSE_VERSION,
                        md_feature_vers=constants.METADATA_FEATURE_VERSION,
                        ts_int=ts,
                        ts_str=time.ctime(ts),
                        host_id=self._rinfo['host-id'],
                        score=score))
        for (k, v) in sorted(lm.iteritems()):
            info += "{0}={1}\n".format(k, str(v['status']))

        info_count = int((len(info) + constants.METADATA_BLOCK_BYTES - 1)
                         / constants.METADATA_BLOCK_BYTES)
        self._log.info("Generated %d blocks:\n%s\n<\\0 padding>\n%s",
                       info_count + 1, data, info)
        data = data.ljust(constants.METADATA_BLOCK_BYTES, '\0')
        info = info.ljust(constants.METADATA_BLOCK_BYTES * info_count, '\0')
        out = data + info
        return out

    def _push_to_storage(self, blocks):
        self._broker.put_stats_on_storage(
            self._metadata_dir,
            constants.SERVICE_TYPE,
            self._config.get(config.ENGINE, config.HOST_ID),
            blocks)

    def _collect_all_host_stats(self):
        all_stats = self._broker.get_stats_from_storage(
            self._metadata_dir,
            constants.SERVICE_TYPE)
        local_ts = time.time()
        for host_str, data in all_stats.iteritems():
            try:
                md = metadata.parse_metadata_to_dict(host_str, data)
            except ex.MetadataError as e:
                self._log.error(
                    str(e),
                    extra=self._get_lf_args(self.LF_MD_ERROR))
                continue

            if md['host-id'] not in self._all_host_stats:
                self._all_host_stats[md['host-id']] = {
                    'first-update': True,
                    'last-update-local-ts': local_ts,
                    'last-update-host-ts': None,
                    'alive': 'unknown',
                    'score': 0,
                    'engine-status': None,
                    'hostname': '(unknown)'}

            if self._all_host_stats[md['host-id']]['last-update-host-ts'] \
                    != md['host-ts']:
                # Track first update in order to accurately judge liveness.
                # If last-update-host-ts is 0, then first-update stays True
                # which indicates that we cannot use this last-update-local-ts
                # as an indication of host liveness.
                if self._all_host_stats[md['host-id']]['last-update-host-ts']:
                    self._all_host_stats[md['host-id']]['first-update'] = False

                self._all_host_stats[md['host-id']]['last-update-host-ts'] = \
                    md['host-ts']
                self._all_host_stats[md['host-id']]['last-update-local-ts'] = \
                    local_ts
                self._all_host_stats[md['host-id']]['score'] = \
                    md['score']
                self._all_host_stats[md['host-id']]['engine-status'] = \
                    md['engine-status']
                self._all_host_stats[md['host-id']]['hostname'] = \
                    md['hostname']

        # All updated, now determine if hosts are alive/updating
        for host_id, attr in self._all_host_stats.iteritems():
            if (attr['last-update-local-ts']
                    + constants.HOST_ALIVE_TIMEOUT_SECS) <= local_ts:
                # Check for timeout regardless of the first-update flag status:
                # a timeout in this case means we read stale data, but still
                # must mark the host as dead.
                # TODO newer sanlocks can report this through get_hosts()
                self._log.error("Host %s (id %d) is no longer updating its"
                                " metadata", attr['hostname'], host_id,
                                extra=self._get_lf_args(self.LF_HOST_UPDATE))
                self._all_host_stats[host_id]['alive'] = False
            elif attr['first-update']:
                self._log.info("Waiting for first update from host %s (id %d)",
                               attr['hostname'], host_id,
                               extra=self._get_lf_args(self.LF_HOST_UPDATE))
            else:
                self._log.info("Host %s (id %d) metadata updated",
                               attr['hostname'], host_id,
                               extra=self._get_lf_args(self.LF_HOST_UPDATE))
                info_str = "{0}".format(attr)
                self._log.info("Host %s (id %d): %s",
                               attr['hostname'], host_id, info_str,
                               extra=self._get_lf_args(
                                   self.LF_HOST_UPDATE_DETAIL))
                self._all_host_stats[host_id]['alive'] = True

    def _perform_engine_actions(self):
        """
        Start or stop engine on current host based on hosts' statistics.
        """
        local_host_id = self._rinfo['host-id']

        if self._all_host_stats[local_host_id]['engine-status'] == 'None':
            self._log.info("Unknown local engine vm status, no actions taken")
            return

        rinfo = {
            'best-engine-status':
            self._all_host_stats[local_host_id]['engine-status'],
            'best-engine-status-host-id': local_host_id,
            'best-score': self._all_host_stats[local_host_id]['score'],
            'best-score-host-id': local_host_id,
        }

        for host_id, stats in self._all_host_stats.iteritems():
            if stats['alive'] == 'unknown':
                # TODO probably unnecessary to wait, sanlock will prevent
                # 2 from starting up accidentally
                self._log.info("Unknown host state for id %d,"
                               " waiting for initialization", host_id)
                return
            elif not stats['alive']:
                continue

            if self._get_engine_status_score(stats['engine-status']) \
                    > self._get_engine_status_score(
                        rinfo['best-engine-status']):
                rinfo['best-engine-status'] = stats['engine-status']
                rinfo['best-engine-status-host-id'] = host_id
            # Prefer local score if equal to remote score
            if stats['score'] > rinfo['best-score']:
                rinfo['best-score'] = stats['score']
                rinfo['best-score-host-id'] = host_id

        # FIXME set maintenance flag
        rinfo['maintenance'] = False

        self._rinfo.update(rinfo)

        yield_ = False
        # Process the states until it's time to sleep, indicated by the
        # state handler returning yield_ as True.
        while not yield_:
            self._log.debug("Processing engine state %s",
                            self._rinfo['current-state'])
            self._rinfo['current-state'], yield_ \
                = self._vm_state_actions[self._rinfo['current-state']]()

        self._log.debug("Next engine state %s",
                        self._rinfo['current-state'])

    def _get_engine_status_score(self, status):
        """
        Convert a string engine/vm status to a sortable numeric score;
        the highest score is a live vm with a healthy engine.
        """
        try:
            return self.engine_status_score_lookup[status]
        except KeyError:
            self._log.error("Invalid engine status: %s", status, exc_info=True)
            return 0

    def _handle_entry(self):
        """
        ENTRY state.  Determine current vm state and switch appropriately.
        """
        local_host_id = self._rinfo['host-id']
        if self._all_host_stats[local_host_id]['engine-status'][:5] == 'vm-up':
            return self.States.ON, False
        else:
            return self.States.OFF, False

    def _handle_off(self):
        """
        OFF state.  Check if any conditions warrant starting the vm, and
        check if it was started externally.
        """
        local_host_id = self._rinfo['host-id']

        if self._rinfo['best-engine-status'][:5] == 'vm-up':
            engine_host_id = self._rinfo['best-engine-status-host-id']
            if engine_host_id == local_host_id:
                self._log.info("Engine vm unexpectedly running locally,"
                               " monitoring vm")
                return self.States.ON, False
            else:
                self._log.info(
                    "Engine vm is running on host %s (id %d)",
                    self._all_host_stats[engine_host_id]['hostname'],
                    engine_host_id,
                    extra=self._get_lf_args(self.LF_ENGINE_HEALTH)
                )
                return self.States.OFF, True

        # FIXME remote db down, other statuses

        # FIXME cluster-wide engine maintenance bit

        if self._rinfo['best-score-host-id'] != local_host_id:
            self._log.info("Engine down, local host does not have best score",
                           extra=self._get_lf_args(self.LF_ENGINE_HEALTH))
            return self.States.OFF, True

        self._log.error("Engine down and local host has best score (%d),"
                        " attempting to start engine VM",
                        self._rinfo['best-score'],
                        extra=self._get_lf_args(self.LF_ENGINE_HEALTH))
        return self.States.START, False

    def _handle_start(self):
        """
        START state.  Power on VM.
        """
        try:
            self._start_engine_vm()
        except Exception as e:
            self._log.error("Failed to start engine VM: %s", str(e))
            # FIXME these sorts of tracking vars could be put in an audit log
            self._rinfo['engine-vm-retry-time'] = int(time.time())
            self._rinfo['engine-vm-retry-count'] += 1
            # TODO mail for error (each time, or after n retries?)
            # OFF handler will retry based on host score
            return self.States.OFF, True
        else:
            self._rinfo['engine-vm-retry-time'] = None
            self._rinfo['engine-vm-retry-count'] = 0
            return self.States.ON, True

    def _start_engine_vm(self):
        # Ensure there isn't any stale VDSM state from a prior VM lifecycle
        self._clean_vdsm_state()

        self._log.info("Starting vm using `%s --vm-start`",
                       constants.HOSTED_ENGINE_BINARY)
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY,
                              '--vm-start'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        self._log.info("stdout: %s", output[0])
        self._log.info("stderr: %s", output[1])
        if p.returncode != 0:
            # FIXME consider removing when we can get vm status from sanlock,
            # if still an issue then the alternative of tracking the time we
            # started the engine might be better than parsing this output
            if output[0].startswith("Virtual machine already exists"):
                self._log.warning("Failed to start engine VM,"
                                  " already running according to VDSM")
                return

            self._log.error("Failed: %s", output[1])
            raise Exception(output[1])

        self._log.error("Engine VM started on localhost")
        return

    def _clean_vdsm_state(self):
        """
        Query VDSM for stats on hosted engine VM, and if there are stats for
        the VM but the VM is not running, attempt to clear them using the
        VDSM 'destroy' verb.  If after 10 tries the state is present, raise
        an exception indicating the error.
        """
        self._log.info("Ensuring VDSM state is clear for engine VM")
        vm_id = self._config.get(config.VM, config.VM_UUID)
        use_ssl = util.to_bool(self._config.get(config.ENGINE,
                                                config.VDSM_SSL))

        for i in range(0, 10):
            # Loop until state is clear or until timeout
            try:
                stats = vdsc.run_vds_client_cmd('0', use_ssl,
                                                'getVmStats', vm_id)
            except ex.DetailedError as e:
                if e.detail == "Virtual machine does not exist":
                    self._log.info("Vdsm state for VM clean")
                    return
                else:
                    raise

            vm_status = stats['statsList'][0]['status'].lower()
            if vm_status == 'powering up' or vm_status == 'up':
                self._log.info("VM is running on host")
                return

            self._log.info("Cleaning state for non-running VM")
            try:
                vdsc.run_vds_client_cmd('0', use_ssl, 'destroy', vm_id)
            except ex.DetailedError as e:
                if e.detail == "Virtual machine does not exist":
                    self._log.info("Vdsm state for VM clean")
                    return
                else:
                    raise
            time.sleep(1)

        raise Exception("Timed out trying to clean VDSM state for VM")

    @handler_cleanup
    def _handle_on(self):
        """
        ON state.  See if the VM was stopped or needs to be stopped.
        """
        local_host_id = self._rinfo['host-id']
        if self._rinfo['best-engine-status'][:5] != 'vm-up':
            self._log.error("Engine vm died unexpectedly")
            return self.States.OFF, False
        elif self._rinfo['best-engine-status-host-id'] != local_host_id:
            self._log.error("Engine vm unexpectedly running on other host")
            return self.States.OFF, True

        # FIXME maintenance bit should cause transition to STOP

        best_host_id = self._rinfo['best-score-host-id']
        if (best_host_id != local_host_id
                and self._rinfo['best-score']
                >= self._all_host_stats[local_host_id]['score']
                + self.MIGRATION_THRESHOLD_SCORE):
            self._log.error("Host %s (id %d) score is significantly better"
                            " than local score, migrating vm",
                            self._all_host_stats[best_host_id]['hostname'],
                            best_host_id)
            self._rinfo['migration-host-id'] = best_host_id
            self._rinfo['migration-status'] = self.MigrationStatus.PENDING
            return self.States.MIGRATE, False

        if self._rinfo['best-engine-status'] == 'vm-up bad-health-status':
            now = int(time.time())
            if 'first-bad-status-time' not in self._rinfo:
                self._rinfo['first-bad-status-time'] = now
            timeout = (constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS
                       - (now - self._rinfo['first-bad-status-time']))
            if timeout > 0:
                self._log.error("Engine VM has bad health status,"
                                " timeout in %d seconds", timeout)
                return self.States.ON, True
            else:
                self._log.error("Engine VM timed out with bad health status"
                                " after %d seconds, restarting",
                                constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS)
                self._rinfo['bad-health-failure-time'] = now
                # FIXME how do we avoid this for cases like vm running fsck?
                return self.States.STOP, False

        self._log.info("Engine vm running on localhost")
        return self.States.ON, True

    def _handle_on_cleanup(self):
        if 'first-bad-status-time' in self._rinfo:
            del self._rinfo['first-bad-status-time']

    @handler_cleanup
    def _handle_stop(self):
        """
        STOP state.  Shut down the locally-running vm.
        """
        local_host_id = self._rinfo['host-id']
        if (self._rinfo['best-engine-status'][:5] != 'vm-up'
                or self._rinfo['best-engine-status-host-id'] != local_host_id):
            self._log.info("Engine vm not running on local host")
            return self.States.OFF, True

        force = False
        if self._rinfo.get('engine-vm-shutdown-time'):
            elapsed = int(time.time()) - self._rinfo['engine-vm-shutdown-time']
            if elapsed > constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS:
                force = True

        try:
            self._stop_engine_vm(force)
        except Exception as e:
            self._log.error("Failed to stop engine VM: %s", str(e))
            # Allow rediscovery of vm state.  Yield in case the state
            # machine ends up immediately in the STOP state again.
            return self.States.ENTRY, True

        if force:
            return self.States.OFF, True
        else:
            if 'engine-vm-shutdown-time' not in self._rinfo:
                self._rinfo['engine-vm-shutdown-time'] = int(time.time())
            return self.States.STOP, True

    def _handle_stop_cleanup(self):
        if 'engine-vm-shutdown-time' in self._rinfo:
            del self._rinfo['engine-vm-shutdown-time']

    def _stop_engine_vm(self, force):
        cmd = '--vm-poweroff' if force else '--vm-shutdown'
        self._log.info("Shutting down vm using `%s %s`",
                       constants.HOSTED_ENGINE_BINARY, cmd)
        p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY, cmd],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = p.communicate()
        self._log.info("stdout: %s", output[0])
        self._log.info("stderr: %s", output[1])
        if (p.returncode != 0
                and not output[0].startswith(
                "Virtual machine does not exist")):
            self._log.error("Failed to stop engine vm with %s %s: %s",
                            constants.HOSTED_ENGINE_BINARY, cmd, output[1])
            raise Exception(output[1])

        self._log.error("Engine VM stopped on localhost")
        return

    @handler_cleanup
    def _handle_migrate(self):
        """
        MIGRATE state.  Move the VM to the destination host.
        """
        vm_id = self._config.get(config.VM, config.VM_UUID)
        best_host_id = self._rinfo['migration-host-id']
        if self._rinfo['migration-status'] == self.MigrationStatus.PENDING:
            try:
                vdsc.run_vds_client_cmd(
                    '0',
                    self._config.get(config.ENGINE, config.VDSM_SSL),
                    'migrate',
                    vmId=vm_id,
                    method='online',
                    src='localhost',
                    dst=best_host_id,
                )
            except:
                self._log.error("Failed to start migration", exc_info=True)
                self._rinfo['migration-status'] = self.MigrationStatus.FAILURE
            else:
                self._log.info("Started migration to host %s (id %d)",
                               self._all_host_stats[best_host_id]['hostname'],
                               best_host_id)
                self._rinfo['migration-status'] \
                    = self.MigrationStatus.IN_PROGRESS

        else:
            res = vdsc.run_vds_client_cmd(
                '0',
                self._config.get(config.ENGINE, config.VDSM_SSL),
                'migrate',
                vmId=vm_id
            )
            self._log.info("Migration status: %s", res['status']['message'])

            if res['status']['message'].startswith('Migration in progress'):
                self._rinfo['migration-status'] \
                    = self.MigrationStatus.IN_PROGRESS
            elif res['status']['message'].startswith('Migration done'):
                self._rinfo['migration-status'] \
                    = self.MigrationStatus.DONE
            else:
                self._rinfo['migration-status'] \
                    = self.MigrationStatus.FAILURE

        self._log.debug("Symbolic migration status is %s",
                        self._rinfo['migration-status'])

        if self._rinfo['migration-status'] == self.MigrationStatus.IN_PROGRESS:
            self._log.info("Continuing to monitor migration")
            return self.States.MIGRATE, True
        elif self._rinfo['migration-status'] == self.MigrationStatus.DONE:
            self._log.info("Migration to host %s (id %d) complete,"
                           " no longer monitoring vm",
                           self._all_host_stats[best_host_id]['hostname'],
                           best_host_id)
            return self.States.OFF, True
        elif self._rinfo['migration-status'] == self.MigrationStatus.FAILURE:
            self._log.error("Migration to host %s (id %d) failed",
                            self._all_host_stats[best_host_id]['hostname'],
                            best_host_id)
            return self.States.STOP, False
        else:
            self._log.error("Unexpected migration state, migration failed")
            return self.States.STOP, False

    def _handle_migrate_cleanup(self):
        if 'migration-host-id' in self._rinfo:
            del self._rinfo['migration-host-id']
        if 'migration-status' in self._rinfo:
            del self._rinfo['migration-status']
