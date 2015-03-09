import logging
import abc
from abc import abstractmethod
import copy

__author__ = 'msivak'
__all__ = ["BaseFSM", "BaseState"]


class FSMLoggerAdapter(logging.LoggerAdapter):
    """This logger adapter adds the fsm and state fields to the
       LogRecord.
    """

    def __init__(self, logger, fsm):
        # super cannot be used here because Python 2.6
        # does not derive logging classes from object
        logging.LoggerAdapter.__init__(self, logger, None)
        self.fsm = fsm

    def process(self, msg, kwargs):
        extra = {
            "fsm": self.fsm,
            "state": self.fsm.state,
            "data": self.fsm.state.data
        }
        kwargs["extra"] = kwargs.get("extra", {})
        kwargs["extra"].update(extra)
        return msg, kwargs


class BaseState(object):
    __metaclass__ = abc.ABCMeta
    __slots__ = ["_data"]

    def __init__(self, data):
        # make a copy of data so this instance is totally self-contained
        self._data = copy.deepcopy(data)

    @property
    def data(self):
        """This property holds the data structure that describes the state.
           It is not read-only protected, but you must not ever change
           it (or anything within)!

           If you want a state with different data, create a new instance.
        """
        return self._data

    @abstractmethod
    def score(self, logger):
        """Compute score for the current state.
        """
        return 0

    @abstractmethod
    def collect(self, fsm, new_data, logger):
        """
        This method can collect additional data that are only interesting
        for a certain state. It gets the currently collected data and has
        to return the updated version. It can modify the structure in place.
        :param logger:
        """
        return new_data

    @abstractmethod
    def consume(self, fsm, new_data, logger):
        """Computes a new state based on the fresh data which were just
           collected by the FSM machine and returns the new
           state + commands for the FSM to perform (refresh, quit)
           as a tuple (new_state, commands1, command2, ...).
           :param fsm:
        """
        return self.__class__(new_data)

    @abstractmethod
    def metadata(self):
        """Returns a structure that will be used for reporting
           additional information.
        """
        return {}


class BaseFSM(object):
    NOWAIT = 1
    WAIT = 2
    QUIT = 3

    class Actions(object):
        def __init__(self, d):
            self.__dict__.update(d)

    def __init__(self, initial_state, initial_data, logger, actions={}):
        self._state = initial_state(initial_data)
        self._logger = FSMLoggerAdapter(logger, self)
        self._actions = self.Actions(actions)

    @property
    def state(self):
        return self._state

    @property
    def logger(self):
        return self._logger

    @property
    def actions(self):
        return self._actions

    def __iter__(self):
        return self

    def refresh(self, old_data):
        return old_data

    def next(self):
        """
        This method returns the edge between states that was traversed and
        the requested sleep time as a tuple
        (old_state, new_state, sleep_time).

        It refreshes the data and passes it to the current state.
        The state then returns a new state to go to + some flags.
        """
        new_data = self.refresh(self._state.data)
        new_data = self._state.collect(self, new_data, self.logger)
        ret = self._state.consume(self, new_data, self.logger)

        old_state = self._state
        self._state, commands = self.decode_consume(ret)

        sleep_time = 10
        for command in commands:
            if command == self.WAIT:
                sleep_time = 10
            elif command == self.NOWAIT:
                sleep_time = 0
            elif command == self.QUIT:
                raise StopIteration

        return old_state, self._state, sleep_time

    @staticmethod
    def decode_consume(consume_return):
        try:
            next_state, commands = consume_return[0], consume_return[1:]
        except TypeError:
            # value is not a tuple
            next_state, commands = consume_return, ()
        except ValueError:
            # value is a tuple with only one element
            next_state, commands = consume_return[0], ()
        return next_state, commands
