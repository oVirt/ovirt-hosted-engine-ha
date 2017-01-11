from ..lib.fsm import BaseState, BaseFSM
from ..lib import log_filter
from ..lib import engine
from ovirt_hosted_engine_ha.lib import upgrade
from . import constants
from .state_decorators import check_local_maintenance, check_timeout
from .state_decorators import check_local_vm_unknown, check_global_maintenance
from .state_data import time as dtime, load_factor
import time

__author__ = 'msivak'


class EngineState(BaseState):
    """
    This is the base class that represents stated in Hosted Engine state
    machine. It should not be instantiated directly.
    """

    __slots__ = ["_score"]

    LF_PENALTY_INT = 60
    MIGRATION_THRESHOLD_SCORE = 800
    LF_ENGINE_HEALTH = 'LF_ENGINE_HEALTH'
    LF_ENGINE_HEALTH_INT = 60

    def __init__(self, data):
        """
        :type data: HostedEngineData
        """
        super(EngineState, self).__init__(data)
        self._score = None

    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        return super(EngineState, self).consume(fsm, new_data, logger)

    def collect(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        return super(EngineState, self).collect(fsm, new_data, logger)

    @staticmethod
    def _float_or_default(value, default):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _penalize_memory(self, vm_mem, lm, logger, score, score_cfg):
        """
        Take score and change it according to the low memory rules.
        The score should not be touched when the VM is in On state,
        so we should probably override it in the relevant class.
        """
        free_mem = self._float_or_default(lm['mem-free'], 0)
        if free_mem < vm_mem:
            logger.info('Penalizing score by %d due to free memory %d'
                        ' being lower than required %d',
                        score_cfg['free-memory-score-penalty'],
                        free_mem,
                        vm_mem,
                        extra=log_filter.lf_args('score-memory',
                                                 self.LF_PENALTY_INT))
            score -= score_cfg['free-memory-score-penalty']

        return score

    def score(self, logger):
        """
        Calculates the host score from current state info.
        The score is later used to decide where engine should run (host
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
        If retries are still occurring amongst the HA hosts at that time, this
        host will again have an opportunity to run the engine VM.

        Score weights:
        1600 - gateway address is pingable
        1000 - host's cpu load is less than 90% of capacity
         600 - host's management network bridge is up
         400 - host has 4GB of memory free to run the engine VM

        Adjustments:
         -50 - subtraction for each failed start-vm retry attempt
           0 - score reset to 0 after ENGINE_RETRY_COUNT attempts,
               until ENGINE_RETRY_EXPIRATION_SECS seconds have passed
        """

        # Use the cache if it is ready
        if self._score is not None:
            return self._score

        lm = self.data.stats.local
        score_cfg = self.data.score_cfg

        score = score_cfg['base-score']

        upgrademgr = upgrade.Upgrade()
        if not upgrademgr.is_conf_file_uptodate():
            score -= score_cfg['not-uptodate-config-penalty']

        # FIXME score needed for vdsm storage pool connection?
        # (depending on storage integration, may not be able to report...)
        if not lm['gateway']:
            logger.info("Penalizing score by %d due to gateway status",
                        score_cfg['gateway-score-penalty'],
                        extra=log_filter.lf_args('score-gateway',
                                                 self.LF_PENALTY_INT))
            score -= score_cfg['gateway-score-penalty']
        if not lm['bridge']:
            logger.info("Penalizing score by %d due to mgmt bridge status",
                        score_cfg['mgmt-bridge-score-penalty'],
                        extra=log_filter.lf_args('score-mgmtbridge',
                                                 self.LF_PENALTY_INT))
            score -= score_cfg['mgmt-bridge-score-penalty']

        # Compute cpu load average (not counting load caused by
        # the engine vm.  The default load penalty is:
        #   load 0-40% : 0
        #   load 40-90%: 0 to 1000 (rising linearly with increasing load)
        #   load 90%+  : 1000
        # Thus, a load of 80% causes an 800 point penalty
        load_average = load_factor(self.data)

        if score_cfg['cpu-load-penalty-max'] \
                == score_cfg['cpu-load-penalty-min']:
            # Avoid divide by 0 in penalty calculation below
            if load_average < score_cfg['cpu-load-penalty-min']:
                penalty = 0
            else:
                penalty = score_cfg['cpu-load-score-penalty']
        else:
            # Penalty is normalized to [0, max penalty] and is linear based on
            # (magnitude of value within penalty range) / (size of range)
            penalty = int(
                (load_average - score_cfg['cpu-load-penalty-min']) /
                (
                    score_cfg['cpu-load-penalty-max'] -
                    score_cfg['cpu-load-penalty-min']
                ) *
                score_cfg['cpu-load-score-penalty']
            )
            penalty = max(0, min(score_cfg['cpu-load-score-penalty'],
                                 penalty))
        if penalty > 0:
            logger.info("Penalizing score by %d due to cpu load",
                        penalty,
                        extra=log_filter.lf_args('score-cpu',
                                                 self.LF_PENALTY_INT))
            score -= penalty

        score = self._penalize_memory(self.data.min_memory_threshold,
                                      lm, logger, score, score_cfg)

        # If too many retries occur, give a less-suited host a chance
        if (
            self.data.engine_vm_retry_count >
            constants.ENGINE_RETRY_MAX_ATTEMPTS
        ):
            logger.info('Score is 0 due to %d engine vm retry attempts',
                        self.data.engine_vm_retry_count,
                        extra=log_filter.lf_args('score-retries',
                                                 self.LF_PENALTY_INT))
            score = 0
        elif self.data.engine_vm_retry_count > 0:
            # Subtracting a small amount each time causes round-robin attempts
            # between hosts that are otherwise equally suited to run the engine
            penalty = (
                score_cfg['engine-retry-score-penalty'] *
                self.data.engine_vm_retry_count
            )
            logger.info('Penalizing score by %d'
                        ' due to %d engine vm retry attempts',
                        penalty, self.data.engine_vm_retry_count,
                        extra=log_filter.lf_args('score-retries',
                                                 self.LF_PENALTY_INT))
            score -= penalty

        score = max(0, score)

        # cache the result
        self._score = score

        return score

    def metadata(self):
        data = {"state": self.__class__.__name__,
                "maintenance": False,
                "stopped": False}
        return data


class AgentStopped(EngineState):
    def score(self, logger):
        return 0

    def metadata(self):
        md = super(AgentStopped, self).metadata()
        md["stopped"] = True
        return md


class LocalMaintenance(EngineState):
    """
    This state is entered any time the host gets to local maintenance state.
    It monitors the environment and once the maintenance is completed,
    the FSM is reinitialized.

    :transition:
    :transition ReinitializeFSM:
    """
    def score(self, logger):
        logger.info('Score is 0 due to local maintenance mode',
                    extra=log_filter.lf_args('score-maintenance',
                                             self.LF_PENALTY_INT))
        return 0

    def metadata(self):
        md = super(LocalMaintenance, self).metadata()
        md["maintenance"] = True
        return md

    @check_local_maintenance(None)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        return ReinitializeFSM(new_data)


class GlobalMaintenance(EngineState):
    """
    This is an idler state that does not do anything while the global
    maintenance mode is enabled.

    :transition:
    :transition LocalMaintenance:
    :transition ReinitializeFSM:
    """
    @check_global_maintenance(None)
    @check_local_maintenance(LocalMaintenance)
    def consume(self, fsm, data, logger):
        """
        :type fsm: BaseFSM
        :type data: HostedEngineData
        :type logger: logging.Logger
        """
        return ReinitializeFSM(data)


class UnknownLocalVmState(EngineState):
    """
    Error state that is used when we are not able to determine the
    status of the local engine VM.

    :transition:
    :transition GlobalMaintenance:
    :transition LocalMaintenance:
    :transition ReinitializeFSM:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(None)
    @check_local_maintenance(LocalMaintenance)
    def consume(self, fsm, data, logger):
        """
        :type fsm: BaseFSM
        :type data: HostedEngineData
        :type logger: logging.Logger
        """
        return ReinitializeFSM(data), fsm.NOWAIT


class ReinitializeFSM(EngineState):
    """
    Determine the best state to start with based on the current
    information about the environment.

    :transition GlobalMaintenance:
    :transition LocalMaintenance:
    :transition UnknownLocalVmState:
    :transition EngineStarting:
    :transition EngineDown:
    """
    def score(self, logger):
        return 0

    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_local_maintenance(LocalMaintenance)
    def consume(self, fsm, data, logger):
        """
        :type fsm: BaseFSM
        :type data: HostedEngineData
        :type logger: logging.Logger
        """
        engine_state = data.stats.local["engine-health"]

        # Cleanup some timers and counters
        data = data._replace(
            engine_vm_shutdown_time=None,
            migration_host_id=None)

        # the engine might be just starting so if we go directly to EngineUp
        # we might end up in EngineUpBadHealth and killing the VM
        # if the engine is already up'n'running then EngineStarting will
        # switch to EngineUp (hopefully) without any side effects
        if engine_state and engine_state["vm"] == "up":
            return EngineStarting(data)
        else:
            return EngineDown(data)


class LocalMaintenanceMigrateVm(LocalMaintenance):
    """
    This state is used in preparation of local maintenance
    when the engine runs locally..
    It tries to migrate it to the best remote host and
    then moves to local maintenance state.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition ReinitializeFSM
    :transition EngineStop:
    :transition EngineMigratingAway:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if new_data.best_score_host:
            destination = new_data.best_score_host["host-id"]
        else:
            return ReinitializeFSM(new_data)

        if fsm.actions.MIGRATE(destination,
                               new_data.best_score_host["hostname"]):
            new_data = new_data._replace(migration_host_id=destination)
            return EngineMigratingAway(new_data)
        else:
            # Migration failed to start
            return EngineStop(new_data)


class EngineUp(EngineState):
    """
    When the engine is up and running locally, this state is used
    to monitor it.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition LocalMaintenanceMigrateVm:
    :transition EngineUnexpectedlyDown:
    :transition EngineMigratingAway:
    :transition EngineStop:
    :transition EngineUpBadHealth:
    :transition:
    """
    def _penalize_memory(self, vm_mem, lm, logger, score, score_cfg):
        # if the vm is up, do not check memory usage
        return score

    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_local_maintenance(LocalMaintenanceMigrateVm, BaseFSM.NOWAIT)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if new_data.best_engine_status["vm"] != "up":
            logger.error("Engine vm died unexpectedly")
            return EngineUnexpectedlyDown(new_data), fsm.NOWAIT
        elif new_data.best_engine_host_id != new_data.host_id:
            logger.error("Engine vm unexpectedly running on host %d",
                         new_data.best_engine_host_id)
            return EngineUnexpectedlyDown(new_data), fsm.NOWAIT
        elif new_data.best_engine_status["detail"] == "migration source":
            logger.info("Engine VM found migrating away")
            return EngineMigratingAway(new_data)
        elif (new_data.best_score_host and
              new_data.best_score_host["host-id"] != new_data.host_id and
              new_data.best_score_host["score"] >= self.score(logger) +
              self.MIGRATION_THRESHOLD_SCORE):
            logger.error("Host %s (id %d) score is significantly better"
                         " than local score, shutting down VM on this host",
                         new_data.best_score_host['hostname'],
                         new_data.best_score_host["host-id"])
            return EngineStop(new_data)
        elif (new_data.best_engine_status["vm"] == "up" and
              new_data.best_engine_status["health"] == "bad"):
            return EngineUpBadHealth(new_data)
        else:
            logger.info("Engine vm running on localhost",
                        extra=log_filter.lf_args(self.LF_ENGINE_HEALTH,
                                                 self.LF_ENGINE_HEALTH_INT))
            return EngineUp(new_data)


class EngineDown(EngineState):
    """
    This state is used when the engine is running elsewhere and the local
    host has nothing to do except wait for the engine host to become
    bad.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition LocalMaintenance:
    :transition EngineStarting:
    :transition:
    :transition EngineStart:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_local_maintenance(LocalMaintenance)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if new_data.best_engine_status["vm"] == "up":
            if (new_data.best_engine_host_id ==
                    new_data.stats.host_id):
                # The engine is unexpectedly running here, start monitoring it
                logger.info("Engine vm unexpectedly running locally,"
                            " monitoring vm")
                # can't go directly up, engine needs a while to settle
                return EngineStarting(new_data),
            else:
                # The engine is running somewhere else
                hostname = new_data.stats.hosts[
                    new_data.best_engine_host_id]['hostname']
                logger.info("Engine vm is running on host %s (id %d)",
                            hostname,
                            new_data.best_engine_host_id,
                            extra=log_filter.lf_args(
                                self.LF_ENGINE_HEALTH,
                                self.LF_ENGINE_HEALTH_INT))
                return EngineDown(new_data),

        # VM is not running, who should be starting it?
        if (
            not new_data.history or (
                new_data.history[0].collect_finish -
                new_data.history[-1].collect_finish
            ) < constants.HOST_ALIVE_TIMEOUT_SECS
        ):
            # we do not have enough data to decide yet..
            logger.info("The engine is not running, but we do not have enough"
                        " data to decide which hosts are alive")
            return EngineDown(new_data), fsm.WAIT
        # there might be more hosts with the same score, so they all
        # might try to start the VM, but only one will get the sanlock
        # so only one VM should be started, but that's better than situation
        # when all have the same score and noone want's to start the vm
        # rhbz#1093638
        elif (new_data.best_score_host is None or
              new_data.best_score_host["score"] <= self.score(logger)):
            # we have the best score at the moment, try starting the engine
            logger.info("Engine down and local host has best score (%d),"
                        " attempting to start engine VM",
                        self.score(logger))
            return EngineStart(new_data), fsm.NOWAIT
        else:
            # somebody else will run the engine
            logger.info("Engine down, local host does not have best score",
                        extra=log_filter.lf_args(self.LF_ENGINE_HEALTH,
                                                 self.LF_ENGINE_HEALTH_INT))
            return EngineDown(new_data), fsm.WAIT


class EngineForceStop(EngineState):
    """
    This state is used to force-stop the local VM. Used only
    if the regular stop procedure did not finish on time or
    you already know that the VM is not running and you have
    to clean up.

    :transition GlobalMaintenance:
    :transition LocalMaintenance:
    :transition UnknownLocalVmState:
    :transition EngineDown:
    :transition ReinitializeFSM:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_maintenance(LocalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if fsm.actions.STOP_VM(force=True):
            return EngineDown(new_data)
        else:
            return ReinitializeFSM(new_data)


class EngineStop(EngineState):
    """
    This state is responsible for stopping the local VM in preparation
    of starting it elsewhere. If the stop action takes too long, it falls
    back to EngineForceStop.

    :transition GlobalMaintenance:
    :transition LocalMaintenance:
    :transition UnknownLocalVmState:
    :transition EngineForceStop:
    :transition:
    :transition ReinitializeFSM:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_maintenance(LocalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_timeout(EngineForceStop, constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS,
                   BaseFSM.WAIT)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if (new_data.best_engine_status["vm"] != "up" or
                new_data.best_engine_host_id != new_data.host_id):
            logger.info("Engine vm not running on local host")
            return EngineDown(new_data)
        elif new_data.timeout_start_time is None:
            if fsm.actions.STOP_VM():
                return EngineStop(new_data), fsm.WAIT
            else:
                return ReinitializeFSM(new_data)
        else:
            elapsed = dtime(new_data) - self.data.timeout_start_time
            logger.info("Waiting on shutdown to complete"
                        " (%d of %d seconds)",
                        elapsed,
                        constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS)
            return EngineStop(new_data), fsm.WAIT

    def metadata(self):
        data = super(EngineStop, self).metadata()
        if self.data.timeout_start_time:
            timeout = (
                self.data.timeout_start_time +
                constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS
            )
            data["timeout"] = time.ctime(timeout)
        return data


class EngineUpBadHealth(EngineUp):
    """
    This state is the same as EngineUp. except it is used only when
    the VM is UP and the engine does not report healthy state.
    If the engine stays in this state too long, the VM is stopped and
    started somewhere else.

    :transition EngineStop:
    :transitions_from EngineUp:
    """
    @check_timeout(EngineStop, constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS)
    def consume(self, fsm, new_data, logger):
        now = dtime(new_data)
        # Use now if the timeout is not yet set
        fail_time = new_data.timeout_start_time or now
        logger.error("Engine VM has bad health status,"
                     " timeout in %d seconds",
                     constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS -
                     (now - fail_time))
        return super(EngineUpBadHealth, self).consume(fsm, new_data, logger)

    def metadata(self):
        data = super(EngineUpBadHealth, self).metadata()
        if self.data.timeout_start_time:
            timeout = (
                self.data.timeout_start_time +
                constants.ENGINE_BAD_HEALTH_TIMEOUT_SECS
            )
            data["timeout"] = time.ctime(timeout)
        return data


class EngineUnexpectedlyDown(EngineState):
    """
    If the VM shut down unexpectedly (user command, died, etc.), drop the
    score to effectively move it to another host. This also serves as a
    shortcut for the user to start host maintenance mode, though it still
    should be set manually lest the score recover after a timeout.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition LocalMaintenance:
    :transition EngineDown:
    :transition EngineUp:
    :transition:
    """

    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_local_maintenance(LocalMaintenance)
    @check_timeout(EngineDown,
                   constants.VM_UNEXPECTED_SHUTDOWN_EXPIRATION_SECS)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if new_data.best_engine_status["vm"] == "up":
            if (new_data.best_engine_host_id ==
                    new_data.stats.host_id):
                # The engine is unexpectedly running here, start monitoring it
                logger.info("Engine vm unexpectedly running locally,"
                            " monitoring vm")
                return EngineUp(new_data)
            else:
                # The engine is running somewhere else
                hostname = new_data.stats.hosts[
                    new_data.best_engine_host_id]['hostname']
                logger.info("Engine vm is running on host %s (id %d)",
                            hostname,
                            new_data.best_engine_host_id,
                            extra=log_filter.lf_args(
                                self.LF_ENGINE_HEALTH,
                                self.LF_ENGINE_HEALTH_INT))
                return EngineUnexpectedlyDown(new_data)
        else:
            logger.info("Engine down, local host does not have best score",
                        extra=log_filter.lf_args(self.LF_ENGINE_HEALTH,
                                                 self.LF_ENGINE_HEALTH_INT))

            return EngineUnexpectedlyDown(new_data)

    def score(self, logger):
        if self.data.timeout_start_time:
            time_str = time.ctime(
                self.data.timeout_start_time + self.data.stats.time_epoch
            )
        else:
            time_str = time.ctime()

        logger.info('Score is 0 due to unexpected vm shutdown at %s',
                    time_str,
                    extra=log_filter.lf_args('score-shutdown',
                                             self.LF_PENALTY_INT))
        return 0

    def metadata(self):
        data = super(EngineUnexpectedlyDown, self).metadata()
        if self.data.timeout_start_time:
            timeout = (
                self.data.timeout_start_time +
                constants.VM_UNEXPECTED_SHUTDOWN_EXPIRATION_SECS
            )
            data["timeout"] = time.ctime(timeout)
        return data


class EngineStart(EngineState):
    """
    This state is responsible for starting the VM on the local machine.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition LocalMaintenance:
    :transition EngineStarting:
    :transition EngineDown:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_local_maintenance(LocalMaintenance)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        retry_count = new_data.engine_vm_retry_count

        if fsm.actions.START_VM():
            new_data = new_data._replace(
                engine_vm_retry_time=dtime(new_data),
                engine_vm_retry_count=0)
            return EngineStarting(new_data)
        else:
            new_data = new_data._replace(
                engine_vm_retry_time=dtime(new_data),
                engine_vm_retry_count=retry_count + 1)
            return EngineDown(new_data)


class EngineStarting(EngineState):
    """
    This state is responsible for starting the VM on the local machine.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition LocalMaintenance:
    :transition EngineUp:
    :transition EngineDown:
    """
    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    @check_local_maintenance(LocalMaintenance)
    @check_timeout(EngineStop, constants.ENGINE_STARTING_TIMEOUT,
                   BaseFSM.WAIT)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """

        # engine is running
        engine_state = new_data.stats.local["engine-health"]
        if engine_state["vm"] == engine.VMState.UP:
            if engine_state["health"] == 'good':
                return EngineUp(new_data)
            else:
                logger.info("VM is powering up..")
                return EngineStarting(new_data)
        if engine_state["vm"] == engine.VMState.ALREADY_LOCKED:
            logger.info("Another host already took over..")
            return EngineForceStop(new_data), fsm.NOWAIT

        return EngineUnexpectedlyDown(new_data)


class EngineMigratingAway(EngineState):
    """
    This state is responsible for monitoring a migration of the engine
    VM to some other machine.

    :transition GlobalMaintenance:
    :transition UnknownLocalVmState:
    :transition:
    :transition EngineDown:
    :transition ReinitializeFSM:
    """
    def collect(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        return new_data._replace(
            migration_result=fsm.actions.MONITOR_MIGRATION()
        )

    @check_global_maintenance(GlobalMaintenance)
    @check_local_vm_unknown(UnknownLocalVmState)
    def consume(self, fsm, new_data, logger):
        """
        :type fsm: BaseFSM
        :type new_data: HostedEngineData
        :type logger: logging.Logger
        """
        if (
            new_data.migration_result and
            'progress' in new_data.migration_result and
            'downtime' not in new_data.migration_result and
            new_data.migration_result['progress'] < 100
        ):
            logger.info("Continuing to monitor migration")
            return EngineMigratingAway(new_data), fsm.WAIT

        elif (
            new_data.migration_result and
            'progress' in new_data.migration_result and
            'downtime' in new_data.migration_result
        ):
            logger.info("Migration to %s complete,"
                        " no longer monitoring vm",
                        new_data.migration_host_id)
            new_data = new_data._replace(migration_result=None,
                                         migration_host_id=None)
            return EngineDown(new_data)
        else:
            # Migration failed
            logger.error("Migration failed: %s", new_data.migration_result)
            return ReinitializeFSM(new_data)
