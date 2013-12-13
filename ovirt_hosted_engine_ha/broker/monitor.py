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

import imp
import logging
import os

from ..lib.exceptions import RequestError


class Monitor(object):
    def __init__(self):
        self._log = logging.getLogger("%s.Monitor" % __name__)
        self._active_submonitors = {}
        self._submonitors = {}

        self._discover_submonitors()

    def _discover_submonitors(self):
        # TODO additional path for custom import, SUBMONITOR_DIR ?
        from . import submonitor_base
        smdir = os.path.join(os.path.dirname(submonitor_base.__file__),
                             "submonitors")
        self._log.info("Searching for submonitors in %s", smdir)
        for filename in (f for f in os.listdir(smdir) if f.endswith('.py')):
            name = filename[:-3]
            # TODO better error handling for __init__ and badly-written files
            module = imp.find_module(name, [smdir])
            sm = imp.load_module(name, *module)
            smname = sm.register()
            self._submonitors[smname] = sm
            self._log.info("Loaded submonitor %s", smname)
        self._log.info("Finished loading submonitors")

    def start_submonitor(self, submonitor_type, options=None):
        """
        Starts a monitoring thread, returning an id to the caller which
        it can use to identify this thread to the monitor manager.

        Returns numeric id of new submonitor
        """
        if submonitor_type not in self._submonitors:
            raise Exception("{0} not a registered submonitor type"
                            .format(submonitor_type))

        # TODO should add a method to index/retrieve running submonitors
        # so multiple copies of the same one (type+options) aren't started
        # (maybe just serialize params to use as index of {pstr: id} dict)
        self._log.info("Starting submonitor %s", submonitor_type)
        try:
            sm = self._submonitors[submonitor_type].Submonitor(
                submonitor_type, None, options)
            sm.start()
        except Exception as e:
            self._log.error("Failed to start submonitor", exc_info=True)
            raise RequestError("failed to start submonitor: {0}"
                               .format(str(e)))

        sm_id = id(sm)
        self._active_submonitors[sm_id] = {"type": submonitor_type,
                                           "instance": sm}
        self._log.info("Started submonitor %s, id %d", submonitor_type, sm_id)
        return sm_id

    def stop_submonitor(self, id):
        """
        Stops submonitor with the given id.
        """
        try:
            sm_type = self._active_submonitors[id]["type"]
        except KeyError:
            raise RequestError("submonitor %d not found".format(id))
        self._log.info("Stopping submonitor %s, id %d", sm_type, id)

        try:
            self._active_submonitors[id]["instance"].stop()
        except Exception as e:
            self._log.error("Failed to stop submonitor %d", id, exc_info=True)
            raise RequestError("failed to stop submonitor: %s", str(e))
        del self._active_submonitors[id]

        self._log.info("Stopped submonitor %s, id %d", sm_type, id)

    def stop_all_submonitors(self):
        """
        Stops all submonitors, used for final cleanup on exit.
        """
        self._log.info("Stopping all submonitors")
        errors = []
        for id in self._active_submonitors.keys():
            try:
                self.stop_submonitor(id)
            except RequestError as e:
                errors.append("{0} - {1}".format(str(id), str(e)))
        if errors:
            raise RequestError("failed to stop submonitors: {0}"
                               .format(";".join(errors)))

    def get_value(self, id):
        """
        Returns the most recent result from the specified submonitor.
        """
        try:
            val = self._active_submonitors[id]["instance"].get_last_result()
        except KeyError:
            raise RequestError("submonitor id not found")

        self._log.debug("Submonitor %s id %d current value: %s",
                        self._active_submonitors[id]["type"], id, val)
        return val
