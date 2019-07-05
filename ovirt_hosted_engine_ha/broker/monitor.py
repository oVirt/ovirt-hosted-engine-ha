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

try:
    from importlib import util as importlibutil
    _use_importlib = True
except ImportError:
    import imp
    _use_importlib = False
import logging
import os
import sys
from ..lib.exceptions import RequestError


class Monitor(object):
    def __init__(self):
        self._log = logging.getLogger("{}.Monitor".format(__name__))
        self._active_submonitors = {}
        self._submonitors = {}

        self._discover_submonitors()

    def _discover_submonitors(self):
        # TODO additional path for custom import, SUBMONITOR_DIR ?
        from . import submonitor_base
        smdir = os.path.join(os.path.dirname(submonitor_base.__file__),
                             "submonitors")
        self._log.info("Searching for submonitors in %s", smdir)
        for filename in (f for f in os.listdir(smdir) if
                         (f.endswith('.py') or f.endswith('.pyc'))):
            name = filename[:filename.rindex('.')]
            if _use_importlib:
                spec = importlibutil.spec_from_file_location(
                    name, smdir + "/" + filename)
                sm = importlibutil.module_from_spec(spec)
                sys.modules[spec.name] = sm
                spec.loader.exec_module(sm)
            else:
                # TODO better error handling for __init__
                # and badly-written files
                module = imp.find_module(name, [smdir])
                sm = imp.load_module(name, *module)
            smname = sm.register()
            self._submonitors[smname] = sm
            self._log.info("Loaded submonitor %s", smname)
        self._log.info("Finished loading submonitors")

    def start_submonitor(self, sm_type, options=None):
        """
        Starts a monitoring thread, returning an id to the caller which
        it can use to identify this thread to the monitor manager.

        Returns numeric id of new submonitor
        """
        if sm_type not in self._submonitors:
            raise Exception("{0} not a registered submonitor type"
                            .format(sm_type))

        # Find if a monitor with the same type is already running
        if sm_type in self._active_submonitors:
            self.stop_submonitor(sm_type)

        self._log.info("Starting submonitor %s", sm_type)
        try:
            sm = self._submonitors[sm_type].Submonitor(
                sm_type, None, options)
            sm.start()
        except Exception as e:
            self._log.error("Failed to start submonitor", exc_info=True)
            raise RequestError("failed to start submonitor: {0}"
                               .format(str(e)))

        self._active_submonitors[sm_type] = sm
        self._log.info("Started submonitor %s", sm_type)
        return sm_type

    def stop_submonitor(self, sm_type):
        """
        Stops submonitor with the given type.
        """
        if sm_type not in self._active_submonitors:
            raise RequestError("submonitor %s not found", sm_type)

        self._log.info("Stopping submonitor %s", sm_type)

        try:
            self._active_submonitors[sm_type].stop()
        except Exception as e:
            self._log.error("Failed to stop submonitor %s", sm_type,
                            exc_info=True)
            raise RequestError("failed to stop submonitor: %s", str(e))
        del self._active_submonitors[sm_type]

        self._log.info("Stopped submonitor %s", sm_type)

    def stop_all_submonitors(self):
        """
        Stops all submonitors, used for final cleanup on exit.
        """
        self._log.info("Stopping all submonitors")
        errors = []
        for sm_type in self._active_submonitors.keys():
            try:
                self.stop_submonitor(sm_type)
            except RequestError as e:
                errors.append("{0} - {1}".format(str(sm_type), str(e)))
        if errors:
            raise RequestError("failed to stop submonitors: {0}"
                               .format(";".join(errors)))

    def get_value(self, sm_type):
        """
        Returns the most recent result from the specified submonitor.
        """
        try:
            val = self._active_submonitors[sm_type].get_last_result()
        except KeyError:
            raise RequestError("submonitor id not found")

        self._log.debug("Submonitor %s current value: %s", sm_type, val)
        return val
