from functools import wraps
from ..lib.fsm import BaseFSM
import state_data
import time

__author__ = 'msivak'


# Helper decorators
def check_local_vm_unknown(destination):
    """
    This decorator checks if the state of the local VM can be determined
    and changes the state to destination if it cannot.
    """
    def decorate(f):
        @wraps(f)
        def check(self, fsm, data, logger):
            """
            :type fsm: BaseFSM
            :type data: HostedEngineData
            :type logger: logging.Logger
            """
            if (data.stats and
                    data.stats.local["engine-health"]["vm"] == "unknown"):
                logger.info("Unknown local engine vm status"
                            " no actions taken")
                if destination is None:
                    return self.__class__(data), fsm.WAIT
                else:
                    return destination(data), fsm.WAIT
            else:
                return f(self, fsm, data, logger)
        return check
    return decorate


def check_global_maintenance(destination):
    """
    This decorator checks if the Global maintenance is enabled.
    It moves the FSM to destination state if it is,
    """
    def decorate(f):
        @wraps(f)
        def check(self, fsm, data, logger):
            """
            :type fsm: BaseFSM
            :type data: HostedEngineData
            :type logger: logging.Logger
            """
            if data.stats and data.stats.cluster.get("maintenance", False):
                logger.info("Global maintenance detected")
                if destination is None:
                    return self.__class__(data), fsm.WAIT
                else:
                    return destination(data), fsm.WAIT
            else:
                return f(self, fsm, data, logger)
        return check
    return decorate


def check_timeout(destination, timeout, wait=BaseFSM.WAIT):
    """
    This decorator monitors the time spent in the decorated state
    and if it exceeds timeout then a transition to a new state
    (destination) is initiated.

    This decorator has to be the first when applied to a state.
    Otherwise the timeout will not be cleared, when another
    decorator changes the state.
    """
    def decorate(f):
        @wraps(f)
        def check(self, fsm, data, logger):
            """
            :type fsm: BaseFSM
            :type data: HostedEngineData
            :type logger: logging.Logger
            """
            now = state_data.time(data)
            if (data.timeout_start_time is not None and
                    data.timeout_start_time + timeout < now):
                data = data._replace(timeout_start_time=None)
                return destination(data), wait

            ret = f(self, fsm, data, logger)
            next_state, _cmds = fsm.decode_consume(ret)

            if next_state.__class__ != self.__class__:
                # Moving to different state, clear timeout
                logger.info("Timeout cleared while transitioning %s -> %s",
                            self.__class__, next_state.__class__)
                data = next_state.data._replace(timeout_start_time=None)
            elif data.timeout_start_time is None:
                # Staying in current state, set timeout if not already set
                logger.info("Timeout set to %s while transitioning %s -> %s",
                            time.ctime(now + data.stats.time_epoch + timeout),
                            self.__class__,
                            next_state.__class__)
                # The timeout start has to be set here because some states
                # use the initial None value to determine if it is the first
                # iteration
                data = data._replace(timeout_start_time=now)
            else:
                return ret

            # Prepare updated state response
            next_state = next_state.__class__(data)
            return (next_state,) + _cmds
        return check
    return decorate


def check_local_maintenance(destination, wait=BaseFSM.WAIT):
    """
    This decorator checks if the Local maintenance is enabled.
    It moves the FSM to destination state if it is,
    """
    def decorate(f):
        @wraps(f)
        def check(self, fsm, data, logger):
            """
            :type fsm: BaseFSM
            :type data: HostedEngineData
            :type logger: logging.Logger
            """
            if data.stats and data.stats.local.get("maintenance", False):
                logger.info("Local maintenance detected")
                if destination is None:
                    return self.__class__(data), wait
                else:
                    return destination(data), wait
            else:
                return f(self, fsm, data, logger)
        return check
    return decorate


def check_metadata_too_new(destination):
    """
    This decorator makes sure the last refresh was able to decode
    all metadata. If it wasn't it changes the state to destination
    and initiates agent shutdown, because upgrade is needed.
    """
    def decorate(f):
        @wraps(f)
        def check(self, fsm, data, logger):
            """
            :type fsm: BaseFSM
            :type data: HostedEngineData
            :type logger: logging.Logger
            """
            if data.stats and data.stats.metadata_too_new:
                logger.error("Unsupported metadata version detected,"
                             " trying to restart the service")
                return destination(data), fsm.QUIT
            else:
                return f(self, fsm, data, logger)
        return check
    return decorate
