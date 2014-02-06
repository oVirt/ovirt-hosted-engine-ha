__author__ = 'msivak'

import collections

# Prepare the state data type as namedtuple to prevent
# accidental modification to it
HostedEngineData = collections.namedtuple("HostedEngineData", (
    # Current local Host ID
    "host_id",

    # Historic data, the newest element is at the 0th index
    "history",
    # Current data (equal to history[0])
    "stats",

    # Timer for @timeout decorator
    "timeout_start_time",

    # Count of recent engine vm startup attempts
    "engine_vm_retry_count",
    # Local timestamp of most recent engine vm startup attempt
    "engine_vm_retry_time",
    # Local timestamp of the last shutdown attempt
    "engine_vm_shutdown_time",
    # Local timestamp when health status caused vm shutdown
    "unexpected_shutdown_time",
    # Local timestamp of last metadata logging
    "last_metadata_log_time",
    # Used by ON, MIGRATE_START, and MIGRATE_MONITOR states, tracks host
    # to which the vm is migrating and the status
    "migration_host_id",
    "migration_result",

    # Score configuration
    "score_cfg",

    # Min free memory threshold
    "min_memory_threshold",

    # Precomputed values for best engine in the cluster
    "best_engine_status",
    "best_engine_host_id",

    # Precomputed value for host with best score in cluster
    "best_score_host"))


StatsData = collections.namedtuple("StatsData", (
    # Flag is set if the local agent discovers metadata too new for it
    # to parse, in which case the agent will shut down the engine VM.
    "metadata_too_new",

    # Cluster global metadata
    "cluster",

    # Id of this host just to make sure
    "host_id",

    # Metadata for remote hosts
    "hosts",

    # Local data
    "local",

    # Maintenance information
    "maintenance",

    # Timestamps
    'collect_start', 'collect_finish'
))


def time(he_data):
    """
    :type he_data: HostedEngineData
    """
    return he_data.stats.collect_finish
