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
import os
import threading
import xmlrpclib

from ..env import config
from ..env import constants
from ..lib import monotonic
from ..lib.exceptions import RequestError
from ..lib.storage_backends import FilesystemBackend, VdsmBackend
from ..lib.storage_backends import StorageBackendTypes
from ..lib.util import aligned_buffer, uninterruptible


class StorageBroker(object):

    DOMAINTYPES = {
        StorageBackendTypes.FilesystemBackend: FilesystemBackend,
        StorageBackendTypes.VdsmBackend: VdsmBackend,
    }

    def __init__(self):
        self._log = logging.getLogger("%s.StorageBroker" % __name__)
        self._config = config.Config(logger=self._log)
        self._storage_access_lock = threading.Lock()
        """
        Hosts state (liveness) history as reported by agents:
        format: {service_type: (timestamp, [<host_id>, <host_id>])}
        """
        self._stats_cache = {}

        # register storage domain info
        sd_uuid = self._config.get(config.ENGINE, config.SD_UUID)
        sp_uuid = self._config.get(config.ENGINE, config.SP_UUID)
        dom_type = self._config.get(config.ENGINE, config.DOMAIN_TYPE)

        try:
            devices = {
                constants.SERVICE_TYPE + constants.MD_EXTENSION:
                    VdsmBackend.Device(
                        self._config.get(config.ENGINE,
                                         config.METADATA_IMAGE_UUID,
                                         raise_on_none=True),
                        self._config.get(config.ENGINE,
                                         config.METADATA_VOLUME_UUID,
                                         raise_on_none=True),
                    ),
                constants.SERVICE_TYPE + constants.LOCKSPACE_EXTENSION:
                    VdsmBackend.Device(
                        self._config.get(config.ENGINE,
                                         config.LOCKSPACE_IMAGE_UUID,
                                         raise_on_none=True),
                        self._config.get(config.ENGINE,
                                         config.LOCKSPACE_VOLUME_UUID,
                                         raise_on_none=True),
                    )
            }
            self._backend = VdsmBackend(sp_uuid, sd_uuid, dom_type, **devices)
            self._backend.connect()
        except Exception as _ex:
            self._log.warn("Can't connect vdsm storage: {0} "
                           .format(str(_ex)))
            raise

    def is_host_alive(self, service_type):
        timestamp, host_list = self._stats_cache.get(service_type, (0, ""))
        # the last report from client is too old, so we don't know
        if monotonic.time() - timestamp > constants.HOST_ALIVE_TIMEOUT_SECS:
            return []  # the data is too old

        return host_list

    def push_hosts_state(self, service_type, data):
        current_time = monotonic.time()
        self._stats_cache[service_type] =\
            (current_time, data)

    def get_all_stats_for_service_type(self, service_type):
        """
        Reads all files in storage_dir for the given service_type, returning a
        space-delimited string of "<host_id>=<hex data>" for each host.
        """
        d = self.get_raw_stats_for_service_type(service_type)
        result = {}

        for host_id in sorted(d.keys()):
            result[str(host_id)] = xmlrpclib.Binary(d.get(host_id))
        return result

    def get_raw_stats_for_service_type(self, service_type):
        """
        Reads all files in storage_dir for the given service_type, returning a
        dict of "host_id: data" for each host

        Note: this method is called from the client as well as from
        self.get_all_stats_for_service_type().
        """
        path, offset = self._backend.filename(service_type)
        self._log.debug("Getting stats for service %s from %s with"
                        " offset %d",
                        service_type, path, offset)

        bs = constants.HOST_SEGMENT_BYTES
        # TODO it would be better if this was configurable
        read_size = bs * (constants.MAX_HOST_ID_SCAN + 1)

        fin = None

        with self._storage_access_lock,\
                aligned_buffer(read_size) as direct_io_buffer:

            try:
                # Use direct I/O if possible, to avoid the local filesystem
                # cache from hiding metadata file updates from other hosts.
                direct_flag = (os.O_DIRECT
                               if self._backend.direct_io else 0)

                f = os.open(path, direct_flag | os.O_RDONLY | os.O_SYNC)
                os.lseek(f, offset, os.SEEK_SET)

                fin = os.fdopen(f, 'r', 0)  # 0 disables unneeded buffer
                fin.readinto(direct_io_buffer)
                data = direct_io_buffer.read(read_size)

            except EnvironmentError as e:
                self._log.error("Failed to read metadata from %s",
                                path, exc_info=True)
                raise RequestError("failed to read metadata: {0}"
                                   .format(str(e)))
            finally:
                # Cleanup
                if fin:
                    fin.close()

        return dict(((i / bs, data[i:i + bs])
                     for i in range(0, len(data), bs)
                     if data[i] != '\0'))

    def put_stats(self, service_type, host_id, data):
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
        path, offset = self._backend.filename(service_type)
        offset += host_id * constants.HOST_SEGMENT_BYTES
        self._log.debug("Writing stats for service %s, host id %d"
                        " to file %s, offset %d",
                        service_type, host_id, path, offset)

        byte_data = data.data
        byte_data = byte_data.ljust(constants.HOST_SEGMENT_BYTES, '\0')

        with self._storage_access_lock,\
                aligned_buffer(len(byte_data)) as direct_io_buffer:
            f = None

            try:
                direct_flag = (os.O_DIRECT
                               if self._backend.direct_io else 0)

                f = os.open(path, direct_flag | os.O_WRONLY | os.O_SYNC)
                os.lseek(f, offset, os.SEEK_SET)

                direct_io_buffer.write(byte_data)
                uninterruptible(os.write, f, direct_io_buffer)

            except EnvironmentError as e:
                self._log.error("Failed to write metadata for host %d to %s",
                                host_id, path, exc_info=True)
                raise RequestError("failed to write metadata: {0}"
                                   .format(str(e)))
            finally:
                if f:
                    os.close(f)

        self._log.debug("Finished")

    def get_service_path(self, service):
        """
        Returns the full path to a file or device that holds the data
        for specified service.

        Client ID is provided by the broker logic.
        """
        return self._backend.filename(service)[0]
