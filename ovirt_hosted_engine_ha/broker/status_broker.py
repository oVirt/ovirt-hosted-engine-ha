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

import errno
import logging
import time

import sanlock

from ..broker import constants as broker_constants
from ..lib import exceptions as ex


class StatusBroker(object):
    def __init__(self, storage_broker):
        self._log = logging.getLogger("%s.StatusBroker" % __name__)

        self._lease_file = storage_broker.get_image_path(
            broker_constants.LOCKSPACE_IMAGE)

        self._host_id = None

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
