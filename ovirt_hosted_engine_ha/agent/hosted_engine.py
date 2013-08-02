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

from . import brokerlink
from . import config
from . import constants
from ..lib import exceptions as ex
from ..lib import log_filter


class HostedEngine(object):
    LF_MD_ERROR = 'LF_MD_ERROR'
    LF_HOST_UPDATE = 'LF_HOST_UPDATE'
    LF_HOST_UPDATE_DETAIL = 'LF_HOST_UPDATE_DETAIL'
    LF_ENGINE_HEALTH = 'LF_ENGINE_HEALTH'

    engine_status_score_lookup = {
        'None': 0,
        'vm-down': 1,
        'vm-up bad-health-status': 2,
        'vm-up good-health-status': 3,
    }

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
        self._local_state = {}
        self._all_host_stats = {}

        self._sd_path = None
        self._metadata_path = None

        self._sanlock_initialized = False

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
                'bridge_name': self._config.get(config.VM, config.BRIDGE_NAME)}
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

    def _get_lf_args(self, lf_class):
        return {'lf_class': lf_class,
                'interval': constants.INTERMITTENT_LOG_INTERVAL_SECS}

    def start_monitoring(self):
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
                self._log.info("Sleeping %d seconds", delay)

            else:
                delay = 10
                self._log.debug("Sleeping %d seconds", delay)

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

        host_id = int(self._config.get(config.ENGINE, config.HOST_ID))
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

        # (re-)initialize retry status variables if either a) they've not
        # been initialized yet, or b) the retry window has expired.
        if ('last-engine-retry' not in self._local_state
                or self._local_state['last-engine-retry'] + time.time()
                < constants.ENGINE_RETRY_EXPIRATION_SECS):
            self._local_state['last-engine-retry'] = 0
            self._local_state['engine-retries'] = 0
            self._log.debug("Cleared retry status")

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
        score -= 50 * self._local_state['engine-retries']

        # If too many retries occur, give a less-suited host a chance
        if self._local_state['engine-retries'] > constants.ENGINE_RETRY_COUNT:
            score = 0

        ts = int(time.time())
        data = ("{md_parse_vers}|{md_feature_vers}|{ts_int}"
                "|{host_id}|{score}|{engine_status}|{name}"
                .format(md_parse_vers=constants.METADATA_PARSE_VERSION,
                        md_feature_vers=constants.METADATA_FEATURE_VERSION,
                        ts_int=ts,
                        host_id=self._config.get(config.ENGINE,
                                                 config.HOST_ID),
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
                        host_id=self._config.get(config.ENGINE,
                                                 config.HOST_ID),
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
                host_id = int(host_str)
            except ValueError:
                self._log.error("Malformed metadata:"
                                " host id '%s' not an integer", host_id)
                continue

            if host_id not in self._all_host_stats:
                self._all_host_stats[host_id] = {
                    'first-update': True,
                    'last-update-local-ts': None,
                    'last-update-host-ts': None,
                    'alive': False,
                    'score': 0,
                    'engine-status': None,
                    'hostname': '(unknown)'}

            if len(data) < 512:
                self._log.error("Malformed metadata for host %d:"
                                " received %d of %d expected bytes",
                                host_id, len(data), 512)
                continue
            data = data[:512].rstrip('\0')
            tokens = data.split('|')
            if len(tokens) < 7:
                self._log.error("Malformed metadata for host %d:"
                                " received %d of %d expected tokens",
                                host_id, len(tokens), 7)
                continue

            try:
                md_parse_vers = int(tokens[0])
            except ValueError:
                self._log.error("Malformed metadata for host %d:"
                                " non-parsable metadata version %s",
                                host_id, tokens[0])
                continue

            if md_parse_vers > constants.METADATA_FEATURE_VERSION:
                self._log.error("Metadata version %s for host %s too new for"
                                " this agent (%s)", md_parse_vers, host_id,
                                constants.METADATA_FEATURE_VERSION,
                                extra=self._get_lf_args(self.LF_MD_ERROR))
                continue

            host_ts = int(tokens[2])
            score = int(tokens[4])
            engine_status = str(tokens[5])  # convert from bytearray
            hostname = str(tokens[6])  # convert from bytearray

            if host_ts != self._all_host_stats[host_id]['last-update-host-ts']:
                # Track first update in order to accurately judge liveness
                if self._all_host_stats[host_id]['last-update-host-ts']:
                    self._all_host_stats[host_id]['first-update'] = False

                self._all_host_stats[host_id]['last-update-host-ts'] = host_ts
                self._all_host_stats[host_id]['last-update-local-ts'] = \
                    local_ts
                self._all_host_stats[host_id]['score'] = score
                self._all_host_stats[host_id]['engine-status'] = engine_status
                self._all_host_stats[host_id]['hostname'] = hostname

        # All updated, now determine if hosts are alive/updating
        for host_id, attr in self._all_host_stats.iteritems():
            if attr['first-update']:
                self._log.info("Watching for update from host %s (id %d)",
                               attr['hostname'], host_id,
                               extra=self._get_lf_args(self.LF_HOST_UPDATE))
            elif (attr['last-update-local-ts']
                  + constants.HOST_ALIVE_TIMEOUT_SECS) <= local_ts:
                # TODO newer sanlocks can report this through get_hosts()
                self._log.error("Host %s (id %d) is no longer updating its"
                                " metadata", attr['hostname'], host_id,
                                extra=self._get_lf_args(self.LF_HOST_UPDATE))
                self._all_host_stats[host_id]['alive'] = False
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
        local_host_id = int(self._config.get(config.ENGINE, config.HOST_ID))
        engine_status = self._all_host_stats[local_host_id]['engine-status']
        engine_status_host_id = local_host_id
        best_score = self._all_host_stats[local_host_id]['score']
        best_score_host_id = local_host_id

        if engine_status == 'None':
            self._log.info("Unknown local engine vm status, no actions taken")
            return

        for host_id, stats in self._all_host_stats.iteritems():
            if self._engine_status_score(stats['engine-status']) \
                    > self._engine_status_score(engine_status):
                engine_status = stats['engine-status']
                engine_status_host_id = host_id
            # Prefer local score if equal to remote score
            if stats['score'] > best_score:
                best_score_host_id = host_id

        if engine_status[:5] == 'vm-up':
            # FIXME timeout for bad-host-status: if up and no engine, try to
            # migrate; if can't migrate, reduce local score and shut down
            self._log.info(
                "Engine vm is running on host %s (id %d)",
                self._all_host_stats[engine_status_host_id]['hostname'],
                engine_status_host_id,
                extra=self._get_lf_args(self.LF_ENGINE_HEALTH)
            )
            return

        # FIXME remote db down, other statuses

        # FIXME cluster-wide engine maintenance bit

        if best_score_host_id != local_host_id:
            self._log.info("Engine down, local host does not have best score",
                           extra=self._get_lf_args(self.LF_ENGINE_HEALTH))
            return

        self._log.error("Engine down and local host has best score (%d),"
                        " attempting to start engine VM", best_score,
                        extra=self._get_lf_args(self.LF_ENGINE_HEALTH))
        try:
            self._start_engine_vm()
        except Exception as e:
            self._log.error("Failed to start engine VM: %s", str(e))
            self._local_state['last-engine-retry'] = time.time()
            self._local_state['engine-retries'] += 1
            # TODO mail for error (each time, or after n retries?)
        else:
            self._local_state['last-engine-retry'] = 0
            self._local_state['engine-retries'] = 0

    def _engine_status_score(self, status):
        """
        Convert a string engine/vm status to a sortable numeric score;
        the highest score is a live vm with a healthy engine.
        """
        try:
            return self.engine_status_score_lookup[status]
        except KeyError:
            self._log.error("Invalid engine status: %s", status, exc_info=True)
            return 0

    def _start_engine_vm(self):
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
        # FIXME record start time in order to track bad-health-status timeout
