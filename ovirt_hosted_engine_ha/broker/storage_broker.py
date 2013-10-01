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

import base64
import io
import logging
import os
import threading

from . import constants
from ..lib.exceptions import RequestError
from ..lib import util


class StorageBroker(object):
    def __init__(self):
        self._log = logging.getLogger("StorageBroker")
        self._storage_access_lock = threading.Lock()

    def get_all_stats_for_service_type(self, storage_dir, service_type):
        """
        Reads all files in storage_dir for the given service_type, returning a
        space-delimited string of "<host_id>=<hex data>" for each host.
        """
        d = self.get_raw_stats_for_service_type(storage_dir, service_type)
        str_list = []
        for host_id in sorted(d.keys()):
            hex_data = base64.b16encode(d.get(host_id))
            self._log.debug("Read for host id %d: %s",
                            host_id, hex_data)
            str_list.append("{0}={1}".format(host_id, hex_data))
        return ' '.join(str_list)

    def get_raw_stats_for_service_type(self, storage_dir, service_type):
        """
        Reads all files in storage_dir for the given service_type, returning a
        dict of "host_id: data" for each host

        Note: this method is called from the client as well as from
        self.get_all_stats_for_service_type().
        """
        self._log.info("Getting stats for service %s from %s",
                       service_type, storage_dir)
        path = os.path.join(storage_dir, self._get_filename(service_type))

        # Use direct I/O if possible, to avoid the local filesystem cache
        # from hiding metadata file updates from other hosts.  For NFS, we
        # don't have to worry about alignment; see man open(2) for details.
        # TODO it would be better if this was configurable
        direct_flag = (os.O_DIRECT if constants.USE_DIRECT_IO else 0)

        bs = constants.HOST_SEGMENT_BYTES
        # TODO it would be better if this was configurable
        read_size = bs * (constants.MAX_HOST_ID_SCAN + 1)

        try:
            with self._storage_access_lock:
                f = os.open(path, direct_flag | os.O_RDONLY)
                data = os.read(f, read_size)
                os.close(f)
        except IOError as e:
            self._log.error("Failed to read metadata from %s",
                            path, exc_info=True)
            raise RequestError("failed to read metadata: {0}".format(str(e)))

        return dict(((i / bs, data[i:i+bs])
                     for i in range(0, len(data), bs)
                     if data[i] != '\0'))

    def put_stats(self, storage_dir, service_type, host_id, data):
        """
        Writes to the storage in file <storage_dir>/<service-type>.metadata,
        storing the hex string data (e.g. 01bc4f[...]) in binary format.
        Data is written at offset 4KiB*host_id.

        In theory, NFS write block sizes and close-to-open cache coherency
        let us get away with with propagating metadata updates through a
        segment of a file shared with other clients who update adjacent
        segments, so long as a) the writes don't overlap, and b) we close
        the file after the write.
        """
        host_id = int(host_id)
        path = os.path.join(storage_dir, self._get_filename(service_type))
        offset = host_id * constants.HOST_SEGMENT_BYTES
        self._log.info("Writing stats for service %s, host id %d"
                       " to file %s, offset %d",
                       service_type, host_id, path, offset)

        byte_data = base64.b16decode(data)
        byte_data = byte_data.ljust(constants.HOST_SEGMENT_BYTES, '\0')
        with self._storage_access_lock:
            f = None
            try:
                util.mkdir_recursive(storage_dir)
                f = io.open(path, "r+b")
                f.seek(offset, os.SEEK_SET)
                f.write(byte_data)
            except IOError as e:
                self._log.error("Failed to write metadata for host %d to %s",
                                host_id, path, exc_info=True)
                raise RequestError("failed to write metadata: {0}"
                                   .format(str(e)))
            finally:
                if f:
                    f.close()

        self._log.debug("Finished")

    def _get_filename(self, service_type):
        # Nothing special yet
        # FIXME should escape special chars before production deployment
        return "{0}.{1}".format(service_type, constants.MD_EXTENSION)
