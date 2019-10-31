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
import time
try:
    import xmlrpc.client as xmlrpc_client
except ImportError:
    import xmlrpclib as xmlrpc_client

from ..broker import constants as broker_constants
from ..env import config
from ..env import config_constants as const
from ..env import constants
from ..lib import monotonic
from ..lib import exceptions as ex
from ..lib.storage_backends import FilesystemBackend, VdsmBackend
from ..lib.storage_backends import StorageBackendTypes
from ..lib.util import aligned_buffer, connect_vdsm_json_rpc, uninterruptible

from vdsm.client import ServerError


class StorageBroker(object):

    DOMAINTYPES = {
        StorageBackendTypes.FilesystemBackend: FilesystemBackend,
        StorageBackendTypes.VdsmBackend: VdsmBackend,
    }

    class DomainMonitorStatus(object):
        NONE = 'NONE'
        PENDING = 'PENDING'
        ACQUIRED = 'ACQUIRED'

    def __init__(self):
        self._log = logging.getLogger("{}.StorageBroker".format(__name__))
        self._config = config.Config(logger=self._log)
        self._storage_access_lock = threading.Lock()

        self._service_space = broker_constants.MD_IMAGE
        self._lock_space = broker_constants.LOCKSPACE_IMAGE
        self._sanlock_acquired = False

        """
        Hosts state (liveness) history as reported by agents:
        format: (timestamp, [<host_id>, <host_id>])
        """
        self._stats_cache = (0, "")

        # register storage domain info
        self.sd_uuid = self._config.get(config.ENGINE, const.SD_UUID)
        self.sp_uuid = self._config.get(config.ENGINE, const.SP_UUID)
        self.dom_type = self._config.get(config.ENGINE, const.DOMAIN_TYPE)

        try:
            devices = {
                self._service_space:
                    VdsmBackend.Device(
                        self._config.get(config.ENGINE,
                                         const.METADATA_IMAGE_UUID,
                                         raise_on_none=True),
                        self._config.get(config.ENGINE,
                                         const.METADATA_VOLUME_UUID,
                                         raise_on_none=True),
                    ),
                self._lock_space:
                    VdsmBackend.Device(
                        self._config.get(config.ENGINE,
                                         const.LOCKSPACE_IMAGE_UUID,
                                         raise_on_none=True),
                        self._config.get(config.ENGINE,
                                         const.LOCKSPACE_VOLUME_UUID,
                                         raise_on_none=True),
                    )
            }
            self._backend = VdsmBackend(self.sp_uuid, self.sd_uuid,
                                        self.dom_type, **devices)
            self._backend.connect()
        except Exception as _ex:
            self._log.warn("Can't connect vdsm storage: {0} "
                           .format(str(_ex)))
            raise

    def is_host_alive(self):
        timestamp, host_list = self._stats_cache
        # the last report from client is too old, so we don't know
        if monotonic.time() - timestamp > constants.HOST_ALIVE_TIMEOUT_SECS:
            return []  # the data is too old

        return host_list

    def push_hosts_state(self, data):
        current_time = monotonic.time()
        self._stats_cache = (current_time, data)

    def get_all_stats(self):
        """
        Reads all files in storage_dir for the given service_type, returning a
        space-delimited string of "<host_id>=<hex data>" for each host.
        """
        d = self.get_raw_stats()
        result = {}

        for host_id in sorted(d.keys()):
            result[str(host_id)] = xmlrpc_client.Binary(d.get(host_id))
        return result

    def get_raw_stats(self):
        """
        Reads all files in storage_dir for the given service_type, returning a
        dict of "host_id: data" for each host

        Note: this method is called from the client as well as from
        self.get_all_stats_for_service_type().
        """
        path, offset = self._backend.filename(self._service_space)
        self._log.debug("Getting stats for service %s from %s with"
                        " offset %d",
                        self._service_space, path, offset)

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

                fin = os.fdopen(f, 'rb', 0)  # 0 disables unneeded buffer
                fin.readinto(direct_io_buffer)
                # due to direct IO we can only read as bytes
                bdata = direct_io_buffer.read(read_size)
                # TODO: fix for IDNA hostnames
                data = bdata.decode()

            except UnicodeDecodeError as e:
                self._log.error("Corrupted metadata from %s",
                                path, exc_info=True)
                raise ex.RequestError("Corrupted read metadata: {0}"
                                      .format(str(e)))

            except EnvironmentError as e:
                self._log.error("Failed to read metadata from %s",
                                path, exc_info=True)
                raise ex.RequestError("failed to read metadata: {0}"
                                      .format(str(e)))
            finally:
                # Cleanup
                if fin:
                    fin.close()

        return dict(((i // bs, data[i:i + bs])
                     for i in range(0, len(data), bs)
                     if data[i] != '\0'))

    def put_stats(self, host_id, data):
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
        path, offset = self._backend.filename(self._service_space)
        offset += host_id * constants.HOST_SEGMENT_BYTES
        self._log.debug("Writing stats for service %s, host id %d"
                        " to file %s, offset %d",
                        self._service_space, host_id, path, offset)

        byte_data = data.data
        byte_data = byte_data.ljust(constants.HOST_SEGMENT_BYTES, b'\0')

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
                raise ex.RequestError("failed to write metadata: {0}"
                                      .format(str(e)))
            finally:
                if f:
                    os.close(f)

        self._log.debug("Finished")

    def get_image_path(self, service):
        """
        Returns the full path to a file or device that holds the data
        for specified service.

        Client ID is provided by the broker logic.
        """
        return self._backend.filename(service)[0]

    def get_sector_size(self):
        return self._backend.sector_size()

    def start_domain_monitor(self, host_id):
        dm_status = self._get_domain_monitor_status()
        if dm_status == self.DomainMonitorStatus.NONE:
            cli = connect_vdsm_json_rpc(
                logger=self._log
            )
            try:
                cli.Host.startMonitoringDomain(
                    sdUUID=self.sd_uuid,
                    hostID=host_id,
                )
            except ServerError:
                self._log.error("Failed to start monitoring domain",
                                exc_info=True)
                raise

            self._log.info("Started VDSM domain monitor for %s", self.sd_uuid)
            dm_status = self._get_domain_monitor_status()

        waited = 0
        while dm_status != self.DomainMonitorStatus.ACQUIRED \
                and waited <= broker_constants.MAX_DOMAIN_MONITOR_WAIT_SECS:
            waited += 5
            time.sleep(5)
            dm_status = self._get_domain_monitor_status()

        if dm_status == self.DomainMonitorStatus.ACQUIRED:
            self._log.debug("VDSM is monitoring domain %s", self.sd_uuid)
        else:
            msg = ("Failed to start monitoring domain"
                   " (sd_uuid={0}, host_id={1}): {2}"
                   .format(self.sd_uuid, host_id,
                           "timeout during domain acquisition"))
            self._log.error(msg)
            raise Exception(msg)

    def stop_domain_monitor(self):
        status = self._get_domain_monitor_status()
        if status != self.DomainMonitorStatus.NONE:
            cli = connect_vdsm_json_rpc(
                logger=self._log
            )
            try:
                cli.Host.stopMonitoringDomain(
                    sdUUID=self.sd_uuid,
                )
            except ServerError as e:
                self._log.info("Failed to stop monitoring domain")
                self._log.info(e)
                return

            self._log.info("Stopped VDSM domain monitor for %s", self.sd_uuid)

    def _get_domain_monitor_status(self):
        try:
            cli = connect_vdsm_json_rpc(
                logger=self._log
            )
            repo_stats = cli.Host.getStorageRepoStats(domains=[self.sd_uuid])
        except ServerError as e:
            msg = ("Failed to get VDSM domain monitor status: {0}"
                   .format(str(e)))
            self._log.error(msg)
            raise

        if self.sd_uuid not in repo_stats:
            status = self.DomainMonitorStatus.NONE
            log_level = logging.INFO
        elif repo_stats[self.sd_uuid]['acquired']:
            status = self.DomainMonitorStatus.ACQUIRED
            log_level = logging.DEBUG
        else:
            status = self.DomainMonitorStatus.PENDING
            log_level = logging.INFO

        self._log.log(log_level, "VDSM domain monitor status: %s", status)
        return status
