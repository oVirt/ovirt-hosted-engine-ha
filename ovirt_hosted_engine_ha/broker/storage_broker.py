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

import errno
import io
import logging
import os
import threading

from ..lib.exceptions import RequestError
from ..lib import util


class StorageBroker(object):
    def __init__(self):
        self._log = logging.getLogger("StorageBroker")
        self._storage_access_lock = threading.Lock()

    def get_all_stats_for_service_type(self, storage_dir, service_type):
        """
        Reads all files in storage_dir for the given service_type, returning a
        space-delimited list of "<hostname>=<hex data>" for each host.
        """
        self._log.info("Getting stats for service %s from %s",
                       service_type, storage_dir)
        prefix_len = len(service_type) + 1
        str_list = []
        with self._storage_access_lock:
            try:
                files = os.listdir(storage_dir)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self._log.warning("Metadata dir %s does not exist",
                                      storage_dir)
                    files = ()
                else:
                    self._log.error("Failed to read metadata from %s",
                                    storage_dir, exc_info=True)
                    raise RequestError("failed to read metadata: {0}"
                                       .format(str(e)))

            for f in files:
                if not f.startswith("{0}_".format(service_type)):
                    continue
                host = f[prefix_len:]
                path = os.path.join(storage_dir, f)
                f_obj = None
                try:
                    f_obj = io.open(path, "r+b")
                    data = f_obj.read()
                except IOError as e:
                    self._log.error("Failed to read metadata from %s",
                                    path, exc_info=True)
                    raise RequestError("failed to read metadata: {0}"
                                       .format(str(e)))
                finally:
                    if f_obj:
                        f_obj.close()

                hex_data = ''.join("{0:02X}".format(ord(x)) for x in data)
                self._log.debug("Read for host %s: %s", host, hex_data)
                str_list.append("{0}={1}".format(host, hex_data))

        return " ".join(str_list)

    def put_stats(self, storage_dir, service_type, host, data):
        """
        Writes to the storage as file <storage_dir>/<service-type>_<host-id>,
        storing the hex string data (e.g. 01bc4f[...]) in binary format.
        """
        path = os.path.join(storage_dir,
                            self._get_filename(service_type, host))
        self._log.info("Writing stats for service %s, host %s to file %s",
                       service_type, host, path)

        byte_data = bytearray.fromhex(data)
        with self._storage_access_lock:
            f_obj = None
            try:
                util.mkdir_recursive(storage_dir)
                # TODO DirectFile (from vdsm) might be helpful for atomicity,
                # or can we get a lock on the file?
                f_obj = io.open(path, "w+b")
                f_obj.write(byte_data)
            except IOError as e:
                self._log.error("Failed to write metadata to %s", path,
                                exc_info=True)
                raise RequestError("failed to write metadata: {0}"
                                   .format(str(e)))
            finally:
                if f_obj:
                    f_obj.close()

        self._log.debug("Finished")

    def _get_filename(self, service_type, host):
        # Nothing special yet
        # FIXME should escape special chars before production deployment
        return "{0}_{1}".format(service_type, host)
