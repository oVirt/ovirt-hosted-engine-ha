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

import logging
import threading
import time


class SubmonitorBase(object):
    """
    Base class from which all submonitors are derived.  Provides a consistent
    API through which to access submonitor functionality.
    """

    # Subclasses can override this to provide different default loop intervals
    _default_interval = 10

    def __init__(self, name, interval=None, options=None):
        if interval is None:
            interval = self._default_interval
        self._name = name
        self._interval = interval
        self._options = options
        self._last_result = None
        self._initialization_status = None
        self._baselog = logging.getLogger("SubmonitorBase")

    def setup(self, options):
        """
        Initial setup; may be overridden by subclass to perform initialization.
        """

    def action(self, options):
        """
        Action for submonitor plugins; must be overridden by subclass.  Each
        submonitor should compute their result, then call self.updateResult().

        Using the default looping implementation provided by this class, it is
        guaranteed that only submonitor threads of the same class will be run
        concurrently.
        """
        raise NotImplementedError("Submonitor plugin error: "
                                  "action method not implemented")

    def teardown(self, options):
        """
        Final teardown; may be overridden by subclass to perform cleanup.
        """

    def update_result(self, result):
        """
        Method for submonitors to call to update their latest result.
        """
        self._last_result = result

    def get_last_result(self):
        """
        Method for callers to retrieve the latest submonitor result.
        """
        return self._last_result

    def start(self):
        """
        Activates this submonitor: starts a thread which will perform the
        action() method of the submonitor every self._interval seconds and
        update the result accordingly.
        """
        self._execute = True
        self._initialized = threading.Event()
        threading.Thread(target=self._worker).start()

        # Wait for status of startup, _initialization_status will be set to
        # True on success or a string on error (thus the explicit 'is True')
        self._initialized.wait()
        if self._initialization_status is True:
            return True
        else:
            raise Exception(self._initialization_status)

    def _worker(self):
        """
        A default implementation to call action() every 'interval' seconds.
        Classes can override this to manage their own looping.
        """
        try:
            self.setup(self._options)
        except Exception as e:
            self._initialization_status = str(e)
            raise
        else:
            self._initialization_status = True
        finally:
            self._initialized.set()

        while self._execute:
            next_execution = time.time() + self._interval
            try:
                self.action(self._options)
            except Exception as e:
                # Don't let the submonitor thread die, just retry later
                self._baselog.error("Error executing submonitor %s, args %r",
                                    self._name, self._options, exc_info=True)
                pass
            time_remaining = next_execution - time.time()
            while time_remaining > 0:
                time.sleep(time_remaining)
                time_remaining = next_execution - time.time()
        self.teardown(self._options)

    def stop(self):
        """
        Stops the active submonitor.  This stop() implementation corresponds
        to the default start() implementation.  Classes overriding start()
        should consider overriding this as well.
        """
        # TODO stop the thread more forcefully
        self._execute = False
