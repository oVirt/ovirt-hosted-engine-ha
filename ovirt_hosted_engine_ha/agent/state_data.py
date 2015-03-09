import collections

__author__ = 'msivak'

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
    "best_score_host",

    # a list of alive hosts as seen on the last refresh()
    "alive_hosts"))


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
    'collect_start', 'collect_finish',

    # Time epoch - difference between monotonic time and the system time
    # it's 0 if we fail to obtain the monotonic time
    'time_epoch'
))


def time(he_data):
    """
    :type he_data: HostedEngineData
    """
    return he_data.stats.collect_finish


def load_factor(he_data):
    """
    Computes the average CPU load over all known history. Uses weighted average
    to account for different time spans between data points. The load intervals
    are integrated using the trapezoidal method:

    area = (b - a)(f(a) + f(b))/2
    where a,b are timestamps and f(a) and f(b) are values
    """

    def trapezoid(acc, point):
        """
        type point: StatsData
        """
        area, time, last_load, last_time = acc
        cur_load = point.local["cpu-load"]
        cur_time = point.collect_start
        if cur_load is not None and last_load is not None:
            seg_time = last_time - cur_time
            seg_area = (seg_time *
                        (last_load + cur_load) / 2.0)
            return area + seg_area, time + seg_time, cur_load, cur_time
        else:
            return area, time, cur_load, cur_time

    load_area, load_time = reduce(trapezoid,
                                  he_data.history,
                                  (0, 0, None, None))[:2]
    if load_time == 0:
        return 0.0
    else:
        return load_area / load_time
