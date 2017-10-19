#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2017 Red Hat, Inc.
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

import collections
import errno
import logging
import os
import threading
import time

import sanlock

from ..broker import constants as broker_constants
from ..lib import exceptions as ex


class StatusBroker(object):
    StateEntry = collections.namedtuple("StateEntry", ["host_id", "data"])

    class StatusStorageThread(threading.Thread):
        def __init__(self, storage_broker, status_broker):
            threading.Thread.__init__(self, name="StatusStorageThread")
            self._log = logging.getLogger("%s.StatusBroker.Update" % __name__)
            self._run = True

            self._storage_broker = storage_broker
            self._status_broker = status_broker

            self._state_update = collections.deque([], 1)
            self._state_update_event = threading.Event()

            self._raw_state = collections.deque([], 1)

        def stop(self):
            self._run = False
            self._state_update.clear()
            self._state_update_event.set()

        @property
        def state(self):
            try:
                return self._raw_state.pop()
            except IndexError:
                return {}

        def post(self, host_id, data):
            self._state_update.append(
                StatusBroker.StateEntry(host_id, data)
            )
            self._state_update_event.set()

        def run(self):
            while self._run:
                self._state_update_event.clear()
                if self._state_update:
                    entry = self._state_update.pop()
                    try:
                        # Host with id 0 has special meaning - we store
                        # global metadata here, so no one owns lock
                        # on that id.
                        if (self._status_broker._inquire_whiteboard_lock() or
                                entry.host_id == 0):
                            self._storage_broker.put_stats(
                                entry.host_id,
                                entry.data
                            )
                    except:
                        self._log.error("Failed to update state.",
                                        exc_info=True)
                try:
                    self._raw_state.append(
                        self._storage_broker.get_raw_stats()
                    )
                except:
                    self._log.error("Failed to read state.",
                                    exc_info=True)
                self._state_update_event.wait(broker_constants.STORAGE_DELAY)

    def __init__(self, storage_broker):
        self._log = logging.getLogger("%s.StatusBroker" % __name__)

        self._storage_broker = storage_broker
        self._lease_file = storage_broker.get_image_path(
            broker_constants.LOCKSPACE_IMAGE)

        self._host_id = None
        self._current_state = {}

        self._log.info("Starting status updating thread")
        self._state_update_thread = StatusBroker.StatusStorageThread(
            self._storage_broker,
            self
        )
        self._state_update_thread.start()

        self._log.info("Status broker initialized.")

    def clean_up(self):
        self._state_update_thread.stop()
        self.release_host_id()

    @property
    def host_id(self):
        if not self._host_id:
            raise ex.HostIdNotLockedError("Host id is not set")
        return self._host_id

    @host_id.setter
    def host_id(self, host_id):
        self._host_id = host_id

    def lock_host_id(self, host_id):
        """
        Stores specified host id and locks it in sanlock lockspace.

        As first step we try to check if lock was acquired earlier for that
        host id. If it was not locked yet, it will try to acquire lock on that
        host id.

        If new host_id is supplied, lock on the old one will be released first.
        :param host_id: host_id to lock
        :return: Trues in case of successful locking.
        """
        if self._host_id and self._host_id != host_id:
            self.release_host_id()

        self.host_id = host_id
        try:
            if not self._inquire_whiteboard_lock():
                self._log.warn("Host id is not locked, trying to lock")
                return self._acquire_whiteboard_lock()
        except:
            self.host_id = None
            return False

    def release_host_id(self):
        self._release_whiteboard_lock()
        self.host_id = None

    def reset_lockspace(self):
        if os.path.exists(self._lease_file):
            sanlock.write_lockspace(lockspace=broker_constants.LOCKSPACE_NAME,
                                    path=self._lease_file,
                                    offset=0)

    def get_stats(self):
        """
        Returns a space-delimited string of "<host_id>=<hex data>"
        or each host.
        """
        raw_state = self._state_update_thread.state

        for host_id in sorted(raw_state.keys()):
            self._current_state[str(host_id)] = raw_state.get(host_id)
        return self._current_state

    def put_stats(self, data, host_id=None):
        # We need to send host id here, as Global Maintenance flag
        # is stored at block with host_id 0
        if host_id is None:
            host_id = self.host_id
        self._current_state[str(host_id)] = data.data
        self._state_update_thread.post(host_id, data)

    def _inquire_whiteboard_lock(self):
        return sanlock.inq_lockspace(broker_constants.LOCKSPACE_NAME,
                                     self.host_id, self._lease_file)

    def _acquire_whiteboard_lock(self):
        self._log.log(logging.DEBUG,
                      "Ensuring lease for lockspace %s, host id %d "
                      "is acquired (file: %s)",
                      broker_constants.LOCKSPACE_NAME, self.host_id,
                      self._lease_file)

        for attempt in range(broker_constants.WAIT_FOR_STORAGE_RETRY):
            try:
                sanlock.add_lockspace(broker_constants.LOCKSPACE_NAME,
                                      self.host_id, self._lease_file)
            except sanlock.SanlockException as e:
                if hasattr(e, 'errno'):
                    if e.errno == errno.EEXIST:
                        self._log.debug("Host already holds lock")
                        break
                    elif e.errno == errno.EINVAL:
                        self._log.error(
                            "cannot get lock on host id {0}: "
                            "host already holds lock on a different"
                            " host id"
                            .format(self.host_id))
                        raise  # this shouldn't happen, so throw the exception
                    elif e.errno == errno.EINTR:
                        self._log.warn("cannot get lock on host id {0}:"
                                       " sanlock operation interrupted"
                                       " (will retry)"
                                       .format(self.host_id))
                    elif e.errno == errno.EINPROGRESS:
                        self._log.warn("cannot get lock on host id {0}:"
                                       " sanlock operation in progress"
                                       "(will retry)"
                                       .format(self.host_id))
                    elif e.errno == errno.ENOENT:
                        self._log.warn("cannot get lock on host id {0}:"
                                       " the lock file '{1}' is missing"
                                       "(will retry)"
                                       .format(self.host_id, self._lease_file))
            else:  # no exception, we acquired the lock
                self._log.info("Acquired lock on host id %d", self.host_id)
                break

            # some temporary problem has occurred (usually waiting for
            # the storage), so wait a while and try again
            self._log.info("Failed to acquire the lock. Waiting '{0}'s before"
                           " the next attempt".
                           format(broker_constants.WAIT_FOR_STORAGE_DELAY))
            time.sleep(broker_constants.WAIT_FOR_STORAGE_DELAY)
        else:  # happens only if all attempts are exhausted
            raise ex.SanlockInitializationError(
                "Failed to initialize sanlock, the number of errors has"
                " exceeded the limit")

        # we get here only if the the lock is acquired
        return True

    def _release_whiteboard_lock(self):
        sanlock.rem_lockspace(broker_constants.LOCKSPACE_NAME,
                              self.host_id, self._lease_file)
