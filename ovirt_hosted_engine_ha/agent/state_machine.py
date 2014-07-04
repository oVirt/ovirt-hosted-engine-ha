__author__ = 'msivak'

from ..lib.fsm import BaseFSM, BaseState
from ..lib import util
from ..lib.util import engine_status_score
from . import constants
import time
from states import ReinitializeFSM
from state_data import HostedEngineData, StatsData


class StartState(BaseState):
    """
    :transition ReinitializeFSM:
    """
    def consume(self, fsm, new_data, logger):
        return ReinitializeFSM(new_data)

    def score(self, logger):
        return 0

    def collect(self, fsm, new_data, logger):
        return new_data

    def metadata(self):
        return {}


class EngineStateMachine(BaseFSM):
    def __init__(self, he, logger, actions):
        """
        :type he: ovirt_hosted_engine_ha.agent.hosted_engine.HostedEngine
        :type logger: logging.Logger
        :type actions: dict[str, T]
        """
        initial_data = HostedEngineData(
            host_id=None,
            history=(),
            stats=None,
            timeout_start_time=None,
            engine_vm_retry_count=0,
            engine_vm_retry_time=None,
            engine_vm_shutdown_time=None,
            unexpected_shutdown_time=None,
            last_metadata_log_time=0,
            migration_host_id=None,
            migration_result=None,
            score_cfg=he.score_config,
            min_memory_threshold=he.min_memory_threshold,
            best_engine_status=None,
            best_engine_host_id=None,
            best_score_host=None
        )

        super(EngineStateMachine, self).__init__(StartState, initial_data,
                                                 logger, actions)
        self.hosted_engine = he

    def refresh(self, old_data):
        """
        Ask hosted engine to provide fresh state information and
        pre-compute all the values we need to use it easily.

        :type old_data: HostedEngineData
        :rtype: HostedEngineData
        """
        # Collect stats
        stats = {
            "collect_start": int(time.time()),
        }

        # Do not refresh if the time has not changed
        if (old_data.stats is not None and
                stats["collect_start"] == old_data.stats.collect_finish):
            return old_data

        stats.update(self.hosted_engine.collect_stats())
        stats.update({
            "collect_finish": int(time.time())
        })

        # Convert to read only structure to prevent accidental changes
        stats = StatsData(**stats)

        # Check if hosts are alive using the stat history and local time
        # do not use host-ts directly as it might be wrong, only check
        # if it changed in the last HOST_ALIVE_TIMEOUT_SECS period.

        # 1) Find the first point in history that has to have older update
        # from all hosts
        historic = None
        for h in old_data.history:
            if (h.collect_finish + constants.HOST_ALIVE_TIMEOUT_SECS <
                    stats.collect_start):
                historic = h
                break

        # 2) If a currently available host is not present in the historic
        # record or its timestamp is different then it is alive
        if historic:
            for hid, st in stats.hosts.iteritems():
                st["alive"] = (hid not in historic.hosts or
                               historic.hosts[hid].get("host-ts", None)
                               != st.get("host-ts", None))

            alive_hosts = [st for hid, st in stats.hosts.iteritems()
                           if st["alive"] if "engine-status" in st]
        else:
            alive_hosts = []

        if alive_hosts:
            # Pre-compute the best remote engine (skip old metadata
            # for local host)
            best_engine = max(alive_hosts,
                              key=lambda st:
                              engine_status_score(st['engine-status']))

            # Pre-compute best remote score (skip old metadata for local host)
            best_score = max(alive_hosts,
                             key=lambda st: st['score'])

        # Prepare changes to the data structure
        new_data = {}

        # Compare the best engine remote values with the local state
        lm = stats.local
        if (not alive_hosts or engine_status_score(lm["engine-health"])
                >= engine_status_score(best_engine["engine-status"])):
            new_data["best_engine_status"] = lm["engine-health"]
            new_data["best_engine_host_id"] = self.hosted_engine.host_id
        else:
            new_data["best_engine_status"] = best_engine["engine-status"]
            new_data["best_engine_host_id"] = best_engine["host-id"]

        # Save the best score remote values
        # we can't compare them because we first need to fully initialize
        # the current state
        new_data["best_score_host"] = best_score if alive_hosts else None

        # Re-initialize retry status variables if the retry window
        # has expired.
        if util.has_elapsed(old_data.engine_vm_retry_time,
                            constants.ENGINE_RETRY_EXPIRATION_SECS):
            new_data['engine_vm_retry_time'] = None
            new_data['engine_vm_retry_count'] = 0
            self.logger.debug("Cleared retry status")

        # log the global status every now and then
        if util.has_elapsed(old_data.last_metadata_log_time,
                            constants.METADATA_LOG_PERIOD_SECS):
            new_data['last_metadata_log_time'] = int(time.time())
            self.logger.info('Global metadata: %s',
                             str(stats.cluster))
            for host_id, attr in stats.hosts.iteritems():
                info_str = "{0}".format(attr)
                self.logger.info("Host %s (id %d): %s",
                                 attr.get('hostname', "<unknown hostname"),
                                 host_id, info_str)

            self.logger.info("Local (id %d): %s",
                             self.hosted_engine.host_id, str(stats.local))

        # create new data object with the newest stats at the
        # beginning, we are not interested in more than
        # 15 second history snapshots
        # namedtuple._replace makes a copy with updated values
        hlimit = stats.collect_start - constants.STATS_HISTORY_SECS
        new_data = old_data._replace(
            history=(stats,) + self.trim_history(old_data.history, hlimit),
            stats=stats,
            host_id=self.hosted_engine.host_id,

            # The rest of changes as computed by the code above
            **new_data)

        return new_data

    @staticmethod
    def trim_history(history, time_limit):
        return tuple(stat for stat in history
                     if stat.collect_start > time_limit)
