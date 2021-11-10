#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2013-2019 Red Hat, Inc.
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

try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import json
import logging
import os
import re
import socket
import subprocess
import time
import binascii

import sanlock

from . import constants
from ..env import config
from ..env import config_constants as const
from ..lib import brokerlink
from ..lib import engine
from ..lib import exceptions as ex
from ..lib import log_filter
from ..lib import metadata
from ..lib import monotonic
from ..lib import util
from ..lib import upgrade
from .state_machine import EngineStateMachine
from .states import AgentStopped

from vdsm.common import exception as vdsm_exception
from vdsm.client import ServerError
from vdsm.virt import vmstatus


class MetadataTooNewError(Exception):
    """
    This exception is raised when the parser determines that the
    metadata version is too new to handle.
    """
    pass


class ServiceNotUpException(Exception):
    """
    This exception is raised when a required service is not up.

    The agent should return to the main loop and try again later.
    """


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


def float_or_none(v):
    if v == "None":
        return None
    else:
        return float(v)


def engine_status(status):
    if status != 'None':
        # Convert the json unicode strings back to ascii:
        # it makes the output and logs much easier to read
        try:
            return dict([(str(k), str(v)) for (k, v)
                        in json.loads(status).items()])
        except (ValueError, AttributeError):
            return {"vm": "unknown", "health": "unknown",
                    "detail": "serialization error"}
    else:
        return None


class HostedEngine(object):
    LF_MD_ERROR = 'LF_MD_ERROR'
    LF_MD_ERROR_INT = 900
    LF_ENGINE_HEALTH = 'LF_ENGINE_HEALTH'
    LF_ENGINE_HEALTH_INT = 60
    LF_GLOBAL_MD_ERROR = 'LF_GLOBAL_MD_ERROR'
    LF_GLOBAL_MD_ERROR_INT = 900
    LF_MAINTENANCE = 'LF_MAINTENANCE'
    LF_MAINTENANCE_INT = 900
    LF_NOTIFY_ERROR = 'LF_NOTIFY_ERROR'
    LF_NOTIFY_ERROR_INT = 300
    LF_BEST_REMOTE_HOST = 'LF_BEST_REMOTE_HOST'
    LF_BEST_REMOTE_HOST_INT = 300

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
        MAINTENANCE = 'MAINTENANCE'

    class MaintenanceMode(object):
        NONE = 'NONE'
        GLOBAL = 'GLOBAL'
        LOCAL = 'LOCAL'

    def __init__(self, shutdown_requested_callback, host_id=None):
        """
        Initialize hosted engine monitoring logic.  shutdown_requested_callback
        is a callback returning True/False depending on whether ha agent
        shutdown has been requested.
        """
        self._log = logging.getLogger("{0}.HostedEngine".format(__name__))
        self._log.addFilter(log_filter.get_intermittent_filter())

        self._shutdown_requested_callback = shutdown_requested_callback
        self._config = config.Config(logger=self._log)

        self._host_id = host_id
        self._hostname = self._get_hostname()

        if self.configured:
            self._score_cfg = self._get_score_config()
            self._required_monitors = self._get_required_monitors()
        else:
            self._score_cfg = {}
            self._required_monitors = []

        self._broker = None

        self._local_monitors = {}
        self.fsm = EngineStateMachine(self, self._log, actions={
            "START_VM": self._start_engine_vm,
            "STOP_VM": self._stop_engine_vm
        })

        self._sanlock_initialized = False
        self._shared_configuration_supported = False

    @property
    def score_config(self):
        return self._score_cfg

    @property
    def min_memory_threshold(self):
        return int(self._config.get(config.VM, const.MEM_SIZE))

    def _get_score_config(self):
        score = {
            'base-score': constants.BASE_SCORE,
            'network-score-penalty': constants.NETWORK_SCORE_PENALTY,
            'mgmt-bridge-score-penalty': constants.MGMT_BRIDGE_SCORE_PENALTY,
            'free-memory-score-penalty': constants.FREE_MEMORY_SCORE_PENALTY,
            'cpu-load-score-penalty': constants.CPU_LOAD_SCORE_PENALTY,
            'engine-retry-score-penalty': constants.ENGINE_RETRY_SCORE_PENALTY,
            'cpu-load-penalty-min': constants.CPU_LOAD_PENALTY_MIN,
            'cpu-load-penalty-max': constants.CPU_LOAD_PENALTY_MAX,
            'not-uptodate-config-penalty':
                constants.NOT_UPTODATE_CONFIG_PENALITY,
        }
        float_keys = set((
            'cpu-load-penalty-min',
            'cpu-load-penalty-max'
        ))

        cfg = configparser.SafeConfigParser()
        cfg.read(constants.AGENT_CONF_FILE)
        try:
            score.update(cfg.items('score'))
        except (configparser.NoOptionError, configparser.NoSectionError):
            pass

        # When these are used they're expected to be numeric types
        for k, v in score.items():
            if k in float_keys:
                score[k] = float(v)
            else:
                score[k] = int(v)

        return score

    def _get_hostname(self):
        """
        Return the name this host should introduce itself as, which must
        match the Common Name in the certificate used by libvirt (usually
        the vdsm certificate).
        """
        cmd = ['openssl', 'x509',
               '-in', constants.VDSM_CERT_FILE,
               '-noout', '-subject']
        self._log.debug("Executing: {0}".format(' '.join(cmd)))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        output = p.communicate()
        stdout = output[0].decode()
        stderr = output[1].decode()

        if p.returncode != 0:
            self._log.info("Certificate not available (%s),"
                           " using hostname to identify host", stderr)
            return socket.gethostname()

        self._log.debug("Certificate subject: %s", stdout)
        res = re.findall(r'/CN=([A-Za-z0-9-_\.]+)', stdout)

        if len(res) and len(res[0]):
            self._log.info("Found certificate common name: %s", res[0])
            return res[0]
        else:
            self._log.info("Certificate common name not found,"
                           " using hostname to identify host")
            return socket.gethostname()

    def _get_required_monitors(self):
        """
        Called by __init__(), see self._required_monitors

        For each entry:
         'field' - field name in the _local_monitors dict, holding details:
                    'id' - id of started submonitor (or None if not started)
                    'status' - last status returned by this monitor
         'monitor' - monitor type, e.g. ping or cpu-load
         'options' - dict of options needed by this monitor
         'type'    - optional function that converts the value from string
                     to some better type
        """
        network_test = 'ping'
        try:
            network_test = self._config.get(
                config.ENGINE,
                const.NETWORK_TEST
            )
        except (KeyError, ValueError):
            pass
        tcp_t_address = None
        tcp_t_port = None
        try:
            tcp_t_address = self._config.get(
                config.ENGINE,
                const.TCP_T_ADDRESS
            )
        except (KeyError, ValueError):
            pass
        try:
            tcp_t_port = self._config.get(
                config.ENGINE,
                const.TCP_T_PORT
            )
        except (KeyError, ValueError):
            pass

        req = []
        req.append({
            'field': 'network',
            'monitor': 'network',
            'type': float,
            'options': {
                'addr': self._config.get(config.ENGINE, const.GATEWAY_ADDR),
                'network_test': network_test,
                'tcp_t_address': tcp_t_address,
                'tcp_t_port': tcp_t_port,
            }
        })
        req.append({
            'field': 'bridge',
            'monitor': 'mgmt-bridge',
            'type': bool,
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, const.VDSM_SSL),
                'bridge_name': self._config.get(
                    config.ENGINE, const.BRIDGE_NAME
                )}
        })
        req.append({
            'field': 'mem-free',
            'monitor': 'mem-free',
            'type': float_or_none,
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, const.VDSM_SSL)}
        })
        req.append({
            'field': 'cpu-load',
            'monitor': 'cpu-load-no-engine',
            'type': float_or_none,
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, const.VDSM_SSL),
                'vm_uuid': self._config.get(config.ENGINE, const.HEVMID)}
        })
        req.append({
            'field': 'engine-health',
            'monitor': 'engine-health',
            'type': engine_status,
            'options': {
                'address': '0',
                'use_ssl': self._config.get(config.ENGINE, const.VDSM_SSL),
                'vm_uuid': self._config.get(config.ENGINE, const.HEVMID)}
        })
        req.append({
            'field': 'storage-domain',
            'monitor': 'storage-domain',
            'type': bool,
            'options': {
                'sd_uuid': self._config.get(config.ENGINE, const.SD_UUID)
            }
        })
        return req

    @property
    def host_id(self):
        if self._host_id is not None:
            return self._host_id

        host_id = self._config.get(config.ENGINE, const.HOST_ID)
        return int(host_id) if host_id else None

    @property
    def configured(self):
        """Hosted engine is configured when host id is present and the
           configured value is not explicitly set to False
        """
        configured = self._config.get(config.ENGINE, const.CONFIGURED)
        return self.host_id and (configured is None or configured == "True")

    def publish(self, state):
        blocks = self._generate_local_blocks(state)
        self._push_to_storage(blocks)
        self.update_hosts_state(state)

    def clean(self, force=False):
        """
        Make sure the metadata storage is connected and publish
        an empty record to purge the old data.
        """

        # Cancel the operation when HE is not fully configured
        if not self.configured:
            self._log.error("Hosted Engine is not configured. Shutting down.")
            return -1

        self._log.debug("Connecting to ha-broker")
        try:
            self._initialize_vdsm()
            self._initialize_broker(monitors=[])
            self._initialize_domain_monitor()
            self._validate_storage_images()
            self._lock_host_id()
        except ServiceNotUpException as e:
            self._log.error("Required service %s is not up.", str(e))
            return -2
        except sanlock.SanlockException:
            if not force:
                raise
            else:
                self._log.warning(
                    "Force requested, overriding sanlock failure."
                )

        data = {}

        try:
            data = self.collect_stats(get_local=True)
        except Exception as e:
            level = self._log.warn if force else self._log.error
            level("Metadata block has wrong format: %s", e)

        try:
            if force or data["hosts"][self.host_id].get("stopped", False):
                self._log.info("Cleaning the metadata block!")
                self._push_to_storage("")
            else:
                self._log.error("Cannot clean unclean metadata block."
                                " Consider --force-clean.")
        except (ValueError, KeyError):
            self._log.error("Metadata for current host missing.")

        # Free lockspace
        self._log.debug("Releasing sanlock")
        try:
            self._broker.release_host_id()
        except sanlock.SanlockException:
            # This could happen when force was in effect
            if not force:
                raise

        return 0

    def start_monitoring(self):
        # Shut down the agent when HE is not fully configured
        if not self.configured:
            self._log.error("Hosted Engine is not configured. Shutting down.")
            return -1

        last_local_maintenance_update = monotonic.time()
        while self._config.get_local_maintenance():
            # HA used to continue operating as normal during local maintenance
            # state, handling it as one of the states in the agent state
            # machine, including updating status on the shared storage.
            # vdsm recently started actively disconnecting shared storage in
            # local maintenance, which prevents this.
            # So: Right on start, before activating shared storage, check for
            # local maintenance, and if so, do nothing, basically.
            # TODO: Make numbers configurable?
            now = monotonic.time()
            if now - last_local_maintenance_update > 30:
                last_local_maintenance_update = now
                self._log.info("Local maintenance set")
            time.sleep(2)

        # make sure everything is initialized
        # VDSM has to be initialized first, because it's needed to prepare the
        # storage domain connection. Then the storage.
        # Broker then initializes the pieces needed for metadata and leases
        # which are then used by sanlock
        # The domain monitor is not yes needed at this stage and we will
        # initialize it once the FSM is started (we need maintenance data
        # to decide)
        self._initialize_vdsm()
        self._initialize_broker()
        self._initialize_domain_monitor()
        self._lock_host_id()

        # check if configuration is up to date, otherwise upgrade (3.5 -> 3.6)
        upg = upgrade.Upgrade()
        upgraded = upg.upgrade_35_36()
        if upgraded:
            self._log.info("Reloading hosted-engine.conf after upgrade")
            self._config = config.Config(logger=self._log)

        self._shared_configuration_supported = \
            upgrade.is_conf_file_uptodate(self._config)

        self._config.refresh_vm_conf()

        try:
            self._monitoring_loop()
        except Exception:
            self._log.error("Unhandled monitoring loop exception",
                            exc_info=True)
        finally:
            # Publish stopped status
            stopped = AgentStopped(self.fsm.state.data)
            self.publish(stopped)

            # Free lockspace
            self._log.debug("Releasing sanlock")
            self._broker.release_host_id()
            self._stop_domain_monitor_if_possible(stopped)

            return 0

    def _monitoring_loop(self):
        # Safe initial value, so first time loop will be fully executed
        prev_delay = 1
        for old_state, state, delay in self.fsm:
            loop_start = monotonic.time()
            if self._shutdown_requested_callback():
                break

            self._log.debug("Processing engine state %s", state)
            if old_state.__class__.__name__ != state.__class__.__name__:
                try:
                    event = brokerlink.NotifyEvents.STATE_TRANSITION
                    self._broker.notify(event,
                                        "{0}-{1}".format(
                                            old_state.__class__.__name__,
                                            state.__class__.__name__),
                                        hostname=socket.gethostname())
                except Exception:
                    self._log.warning("Could not send notification. Ignoring"
                                      "the error and continuing.",
                                      extra=log_filter.lf_args(
                                          self.LF_NOTIFY_ERROR,
                                          self.LF_NOTIFY_ERROR_INT))
                    self._log.debug("Detailed explanation of notification"
                                    " failure", exc_info=True)

            try:
                if prev_delay > 0:
                    # make sure everything is still initialized
                    self._initialize_vdsm()
                    self._config.refresh_vm_conf()
                    self._lock_host_id()

                # stop the VDSM domain monitor in local maintenance, but
                # only when the VM is not running locally.
                # Checking storage validity in LocalMaintenance is useless,
                # due to lack of the storage monitoring.
                st = state.data.stats
                if st and not st.local.get("maintenance", False):
                    self._initialize_domain_monitor()
                    self._validate_storage_images()
                else:
                    self._stop_domain_monitor_if_possible(state)

                # log state
                self._log.info("Current state %s (score: %d)",
                               state.__class__.__name__,
                               state.score(self._log))
                if state.data.best_score_host:
                    self._log.info("Best remote host %s (id: %d, score: %d)",
                                   state.data.best_score_host["hostname"],
                                   state.data.best_score_host["host-id"],
                                   state.data.best_score_host["score"],
                                   extra=log_filter.lf_args(
                                       self.LF_BEST_REMOTE_HOST,
                                       self.LF_BEST_REMOTE_HOST_INT,
                                   ))

                # publish the current state
                self.publish(state)

            except ServiceNotUpException as e:
                self._log.info("Required service {} is not up.".format(str(e)))
                delay = max(delay, 30)

            loop_stop = monotonic.time()
            self._log.debug("Monitoring loop execution time %d sec",
                            loop_stop - loop_start)

            prev_delay = delay

            delay = max(0, delay - loop_stop + loop_start)
            self._log.debug("Sleeping %d seconds", delay)
            time.sleep(delay)

    def _initialize_broker(self, monitors=None):
        if self._broker:
            return
        self._log.info("Initializing ha-broker connection")
        if not self._broker:
            self._broker = brokerlink.BrokerLink()

        if monitors is not None:
            required_monitors = monitors
        else:
            required_monitors = self._required_monitors

        for m in required_monitors:
            try:
                lm = {}
                lm['id'] = self._broker.start_monitor(m['monitor'],
                                                      m.get('options', {}))
                lm['type'] = m['type'] if 'type' in m else None
            except ex.RequestError:
                self._log.error("Failed to start necessary monitors")
                # Stopping monitors will occur automatically upon disconnection
                raise
            else:
                self._local_monitors[m['field']] = lm

        self._log.info("Broker initialized, all submonitors started")

    def _initialize_vdsm(self):
        self._log.debug("Initializing VDSM")
        tries = 0

        while tries < constants.MAX_VDSM_START_RETRIES:
            tries += 1
            try:
                self._check_service('vdsmd')
                break
            except ServiceNotUpException:
                raise
            except Exception as _ex:
                if tries > constants.MAX_VDSM_START_RETRIES:
                    self._log.error("Can't start vdsmd, the number of errors "
                                    "has exceeded the limit: '{0}'"
                                    .format(_ex))
                    raise
                self._log.warn("Can't start vdsmd, waiting '{0}' seconds "
                               "before the next attempt"
                               .format(constants.MAX_VDSM_WAIT_SECS))
                time.sleep(constants.MAX_VDSM_WAIT_SECS)

        util.connect_vdsm_json_rpc(
            logger=self._log
        )

    def _validate_storage_images(self):
        if not self._broker.get_monitor_status('storage-domain'):
            self._log.warn("Hosted-engine storage domain is in invalid state")
            raise ex.StorageDisconnectedError(
                "Hosted-engine storage domain is in invalid state")

    def _check_service(self, service_name):
        self._log.debug("Checking %s status", service_name)
        with open(os.devnull, "w") as devnull:
            p = subprocess.Popen(['service', service_name, 'status'],
                                 stdout=devnull, stderr=devnull)
            if p.wait() == 0:
                self._log.debug("%s running", service_name)
            else:
                # Wait for the service to start properly
                raise ServiceNotUpException(service_name)

    def _lock_host_id(self):
        self._check_service('sanlock')
        self._broker.lock_host_id(self.host_id)

    def _stop_domain_monitor_if_possible(self, state):
        # make sure the VM is not running locally, stopping the monitor
        # would kill it (treat the VM as up when no data, better
        # keep the monitor running if not sure)
        lm = state.data.stats.local
        if lm.get("engine-health", {})\
                .get("vm", engine.VMState.UP) == engine.VMState.UP:
            self._log.warn("The VM is running locally or we have no data,"
                           " keeping the domain monitor.")
        else:
            self._stop_domain_monitor()

    def _stop_domain_monitor(self):
        try:
            self._broker.stop_domain_monitor()
        except ServerError as e:
            self._log.info("Failed to stop monitoring domain")
            self._log.info(e)
            return

        self._log.info("Stopped VDSM domain monitor",)

    def _initialize_domain_monitor(self):
        self._broker.start_domain_monitor(self.host_id)

    def _generate_local_blocks(self, state):
        """
        This method places the current state and score on shared storage
        in the following format:

          {md_parse_vers}|{md_feature_vers}|{ts_int}
            |{host_id}|{score}|{engine_status}|{name}

        The compiled block is read back from the storage by other hosts,
        parsed from the string above, and used in the state machine logic.
        Most importantly to determine where the engine should be running.
        """
        score = state.score(self.fsm.logger)
        lm = state.data.stats.local
        md = state.metadata()

        tokens = []
        # Metadata lowest compatible version
        tokens.append(constants.METADATA_PARSE_VERSION)
        # Metadata highest compatible version
        tokens.append(constants.METADATA_FEATURE_VERSION)
        # System timestamp
        tokens.append(state.data.stats.collect_start)
        # Host ID
        tokens.append(state.data.stats.host_id)
        # Host score
        tokens.append(score)
        # Engine status
        tokens.append(json.dumps(lm['engine-health']))
        # System hostname
        tokens.append(self._hostname)
        # Local maintenance flag
        tokens.append(1 if md["maintenance"] else 0)
        # Agent stopped cleanly flag
        tokens.append(1 if "stopped" in md and md["stopped"] else 0)
        # CRC32 in hex (use 0 for computing the crc)
        tokens.append(metadata.EMPTY_CRC32)

        # Configuration on the shared storage (>=3.6) is supported
        tokens.append(1 if self._shared_configuration_supported else 0)
        # Timestamp of the latest vm.conf correct refresh
        tokens.append(self._config.vm_conf_refresh_time)

        data = ("|".join(str(t) for t in tokens)).encode()
        crc32 = metadata.CRC32_FORMAT % (binascii.crc32(data) & 0xffffffff)
        tokens[9] = crc32
        data = "|".join(str(t) for t in tokens)

        if len(data) > constants.METADATA_BLOCK_BYTES:
            raise Exception("Output metadata too long ({0} bytes)"
                            .format(data))

        info = ("metadata_parse_version={md_parse_vers}\n"
                "metadata_feature_version={md_feature_vers}\n"
                "timestamp={ts_int} ({ts_str})\n"
                "host-id={host_id}\n"
                "score={score}\n"
                "vm_conf_refresh_time={vm_ts_int} ({vm_ts_str})\n"
                "conf_on_shared_storage={conf_on_shared_storage}\n"
                .format(md_parse_vers=constants.METADATA_PARSE_VERSION,
                        md_feature_vers=constants.METADATA_FEATURE_VERSION,
                        ts_int=state.data.stats.collect_start,
                        ts_str=time.ctime(state.data.stats.collect_start +
                                          state.data.stats.time_epoch),
                        host_id=state.data.host_id,
                        score=score,
                        vm_ts_int=self._config.vm_conf_refresh_time,
                        vm_ts_str=time.ctime(
                            self._config.vm_conf_refresh_time +
                            self._config.vm_conf_refresh_time_epoch
                        ),
                        conf_on_shared_storage='True'
                        if self._shared_configuration_supported else 'False'
                        ))
        # state | metadata
        for (k, v) in sorted(md.items()):
            info += "{0}={1}\n".format(k, str(v))

        info_count = int((len(info) + constants.METADATA_BLOCK_BYTES - 1) /
                         constants.METADATA_BLOCK_BYTES)
        self._log.debug("Generated %d blocks:\n%s\n<\\0 padding>\n%s",
                        info_count + 1, data, info)
        data = data.ljust(constants.METADATA_BLOCK_BYTES, '\0')
        info = info.ljust(constants.METADATA_BLOCK_BYTES * info_count, '\0')
        out = data + info
        return out

    def _push_to_storage(self, blocks):
        self._broker.put_stats_on_storage(self.host_id, blocks)

    def update_hosts_state(self, engine_state):
        self._broker.put_hosts_state_on_storage(self.host_id,
                                                engine_state.data.alive_hosts)

    def collect_stats(self, get_local=False):

        data = {
            # Flag is set if the local agent discovers metadata too new for it
            # to parse, in which case the agent will shut down the engine VM.
            "metadata_too_new": False,

            # Global metadata
            "cluster": {},

            # Id of this host just to make sure
            "host_id": self.host_id,

            # Metadata for remote hosts
            "hosts": {},

            # Local data
            "local": {},

            # Maintenance information
            "maintenance": False,
        }

        all_stats = self._broker.get_stats_from_storage()

        # host_id 0 is a special case, representing global metadata
        if all_stats and 0 in all_stats:
            data["cluster"] = self.process_global_metadata(all_stats.pop(0))

        # collect the last reported state for all hosts
        for host_id, remote_data in all_stats.items():
            try:
                # we are not interested in stale data about local
                # machine
                if host_id == self.host_id and not get_local:
                    continue
                stats = self.process_remote_metadata(host_id, remote_data)
                data["hosts"][host_id] = stats
            except MetadataTooNewError:
                data["metadata_too_new"] = True

        # collect all local stats
        self._log.debug("Refreshing all submonitors")
        for field, monitor in self._local_monitors.items():
            ret = self._broker.get_monitor_status(monitor['id'])
            if ret == 'False':
                ret = False
            elif monitor['type'] is not None:
                ret = monitor['type'](ret)
            data["local"][field] = ret

        # check local maintenance
        self._config.refresh_local_conf_file(config.HA)
        manual_maintenance = util.to_bool(self._config.get(
            config.HA,
            const.LOCAL_MAINTENANCE_MANUAL))
        local_maintenance = util.to_bool(self._config.get(
            config.HA,
            const.LOCAL_MAINTENANCE))
        data["local"]["maintenance"] = manual_maintenance or local_maintenance

        self._log.debug("Refresh complete")

        return data

    def process_remote_metadata(self, host_id, data):
        try:
            md = metadata.parse_metadata_to_dict(host_id, data)
            # Make sure the Id database is consistent
            assert md["host-id"] == host_id
        except ex.FatalMetadataError as e:
            self._log.error(
                str(e),
                extra=log_filter.lf_args(self.LF_MD_ERROR + str(host_id),
                                         self.LF_MD_ERROR_INT))
            raise MetadataTooNewError()
        except ex.MetadataError as e:
            self._log.error(
                str(e),
                extra=log_filter.lf_args(self.LF_MD_ERROR + str(host_id),
                                         self.LF_MD_ERROR_INT))
            return {}
        except AssertionError as e:
            # Ignore host if the Id is not consistent
            self._log.error(
                str(e),
                extra=log_filter.lf_args(self.LF_MD_ERROR + str(host_id),
                                         self.LF_MD_ERROR_INT))
            return {}
        else:
            md['engine-status'] = engine_status(md["engine-status"])
            return md

    def process_global_metadata(self, data):
        md = {}
        if data is not None:
            try:
                md = metadata.parse_global_metadata_to_dict(self._log, data)
            except ex.MetadataError as e:
                self._log.error(
                    str(e),
                    extra=log_filter.lf_args(self.LF_GLOBAL_MD_ERROR,
                                             self.LF_GLOBAL_MD_ERROR_INT))
                # Continue agent processing, ignoring the bad global metadata
        return md

    def _start_engine_vm(self):
        try:
            self._config.refresh_vm_conf()

            # Ensure there isn't any stale VDSM state from a prior VM lifecycle
            self._clean_vdsm_state()

            self._log.info(
                "Starting vm using `%s --vm-start`",
                constants.HOSTED_ENGINE_BINARY
            )
            p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY,
                                  '--vm-start'],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            output = p.communicate()
            stdout = output[0].decode()
            stderr = output[1].decode()
            self._log.info("stdout: %s", stdout)
            self._log.info("stderr: %s", stderr)
            if p.returncode != 0:
                # FIXME consider removing, we can get vm status from sanlock,
                # if still an issue then the alternative tracking the time we
                # started the engine might be better than parsing this output
                if stdout.startswith("VM exists"):
                    self._log.warning("Failed to start engine VM,"
                                      " already running according to VDSM")
                    return True

                raise Exception(stderr)

            self._log.info("Engine VM started on localhost")
            return True
        except Exception as e:
            self._log.info("Failed to start engine VM: '%s'. Please check the"
                           " vdsm logs. The possible reason: the engine has"
                           " been already started on a different host so this"
                           " one has failed to acquire the lock and it will"
                           " sync in a while."
                           " For more information please visit: "
                           "http://www.ovirt.org/Hosted_Engine_Howto"
                           "#EngineUnexpectedlyDown", str(e))
            return False

    def _clean_vdsm_state(self):
        """
        Query VDSM for stats on hosted engine VM, and if there are stats for
        the VM but the VM is not running, attempt to clear them using the
        VDSM 'destroy' verb.  If after 10 tries the state is present, raise
        an exception indicating the error.
        """
        self._log.info("Ensuring VDSM state is clear for engine VM")
        vm_id = self._config.get(config.VM, const.VM_UUID)

        for i in range(0, 10):
            # Loop until state is clear or until timeout
            cli = util.connect_vdsm_json_rpc(
                logger=self._log
            )
            try:
                stats = cli.VM.getStats(vmID=vm_id)[0]
            except ServerError as e:
                if e.code == vdsm_exception.NoSuchVM.code:
                    self._log.info("Vdsm state for VM clean")
                    return
                raise

            if stats.get('status') in (vmstatus.POWERING_UP, vmstatus.UP):
                self._log.info("VM is running on host")
                return

            self._log.info("Cleaning state for non-running VM")
            cli = util.connect_vdsm_json_rpc(
                logger=self._log
            )
            try:
                cli.VM.destroy(vmID=vm_id)
            except ServerError as e:
                if e.code == vdsm_exception.NoSuchVM.code:
                    self._log.info("Vdsm state for VM clean")
                    return
                raise
            time.sleep(1)

        raise Exception("Timed out trying to clean VDSM state for VM")

    def _stop_engine_vm(self, force=False):
        try:
            cmd = '--vm-poweroff' if force else '--vm-shutdown'
            self._log.info("Shutting down vm using `%s %s`",
                           constants.HOSTED_ENGINE_BINARY, cmd)
            p = subprocess.Popen([constants.HOSTED_ENGINE_BINARY, cmd],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            output = p.communicate()
            stdout = output[0].decode()
            stderr = output[1].decode()
            self._log.info("stdout: %s", stdout)
            self._log.info("stderr: %s", stderr)
            if (p.returncode != 0 and
                    not stdout.startswith(
                    "Virtual machine does not exist")):
                self._log.error("Failed to stop engine vm with %s %s: %s",
                                constants.HOSTED_ENGINE_BINARY, cmd, stderr)
                raise Exception(stderr)

            self._log.error("Engine VM stopped on localhost")
            return True
        except Exception as e:
            self._log.error("Failed to stop engine VM: %s", str(e))
            return False
