#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2015 Red Hat, Inc.
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

from ..env import config
from ..env import constants
from ..env import path as env_path
from ovirt_hosted_engine_ha.lib import exceptions as ex
from ovirt_hosted_engine_ha.lib import util
import logging
import os
from . import log_filter


logger = logging.getLogger(__name__)


class StorageServer(object):

    def __init__(self):
        self._log = logging.getLogger("%s.StorageServer" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())
        self._config = config.Config(logger=self._log)
        self._domain_type = self._config.get(config.ENGINE, config.DOMAIN_TYPE)
        self._spUUID = self._config.get(config.ENGINE, config.SP_UUID)
        self._sdUUID = self._config.get(config.ENGINE, config.SD_UUID)
        self._storage = self._config.get(config.ENGINE, config.STORAGE)
        self._connectionUUID = self._config.get(
            config.ENGINE,
            config.CONNECTIONUUID
        )

        self._iqn = self._config.get(config.ENGINE, config.ISCSI_IQN)
        self._portal = self._config.get(config.ENGINE, config.ISCSI_PORTAL)
        self._user = self._config.get(config.ENGINE, config.ISCSI_USER)
        self._password = self._config.get(config.ENGINE, config.ISCSI_PASSWORD)
        self._port = self._config.get(config.ENGINE, config.ISCSI_PORT)
        self._mnt_options = None
        try:
            self._mnt_options = self._config.get(
                config.ENGINE,
                config.MNT_OPTIONS
            )
        except (KeyError, ValueError):
            pass

    def _get_conlist_nfs_gluster(self):
        conDict = {
            'connection': self._storage,
            'user': 'kvm',
            'id': self._connectionUUID,
        }
        if self._domain_type == constants.DOMAIN_TYPE_NFS3:
            storageType = constants.STORAGE_TYPE_NFS
            conDict['protocol_version'] = 3
        elif self._domain_type == constants.DOMAIN_TYPE_NFS4:
            storageType = constants.STORAGE_TYPE_NFS
            conDict['protocol_version'] = 4
        elif self._domain_type == constants.DOMAIN_TYPE_GLUSTERFS:
            storageType = constants.STORAGE_TYPE_GLUSTERFS
            conDict['vfs_type'] = 'glusterfs'
        conList = [conDict]
        return conList, storageType

    def _fix_filebased_connection_path(self):
        path = os.path.normpath(self._storage)
        if path != self._storage:
            self._log.warning(
                (
                    "Fixing path syntax: "
                    "replacing '{original}' with '{fixed}'"
                ).format(
                    original=self._storage,
                    fixed=path,
                )
            )
        return path

    def _validate_pre_connected_path(self, cli, path):
        """
        On 3.5 we allow the user to deploy on 'server:/path/' allowing a
        trailing '/' and in that case VDSM simply mounts on a different path
        with a trailing '_'. Now, since the engine is going to re-mount it
        without the trailing '/', we have also to canonize that path but,
        in this way, we are going to mount twice if that NFS storage server
        has been already mounted on the wrong path by a previous run of
        old code. This method, if the hosted-engine storage domain is already
        available, checks what we expect against the actual mount path and,
        if they differ, raises
        hosted_engine.DuplicateStorageConnectionException
        See rhbz#1300749
        :param cli:  a jsonrpcvdscli instance
        :param path: the path (without the trailing '/') to be validated
                     against already connected storage server
        :raise       hosted_engine.DuplicateStorageConnectionException on error
                     to prevent connecting twice the hosted-engine storage
                     server
        """
        response = cli.getStorageDomainInfo(self._sdUUID)
        if response['status']['code'] != 0:
            self._log.debug(
                'Storage domain {sd} is not available'.format(sd=self._sdUUID)
            )
        else:
            # verifying only if the storage domain is already connected
            canonical_path = env_path.canonize_file_path(
                self._domain_type,
                path,
                self._sdUUID,
            )
            effective_path = env_path.get_domain_path(self._config)
            if effective_path != canonical_path:
                msg = (
                    "The hosted-engine storage domain is already "
                    "mounted on '{effective_path}' with a path that is "
                    "not supported anymore: the right path should be "
                    "'{canonical_path}'."
                ).format(
                    canonical_path=canonical_path,
                    effective_path=effective_path,
                )
                self._log.error(msg)
                raise ex.DuplicateStorageConnectionException(msg)

    def _get_conlist_iscsi(self):
        storageType = constants.STORAGE_TYPE_ISCSI
        conList = []
        ip_port_list = [
            {'ip': x[0], 'port': x[1]} for x in zip(
                self._storage.split(','),
                self._port.split(',')
            )
        ]
        for x in ip_port_list:
            conList.append({
                'connection': x['ip'],
                'iqn': self._iqn,
                'tpgt': self._portal,
                'user': self._user,
                'password': self._password,
                'id': self._connectionUUID,
                'port': x['port'],
            })
        return conList, storageType

    def _get_conlist_fc(self):
        storageType = constants.STORAGE_TYPE_FC
        conList = []
        return conList, storageType

    def _check_connection(self, status):
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Connection to storage server failed: %s' %
                status['status']['message']
            )
        cl = status['items'] if 'items' in status else []
        for con in cl:
            if con['status'] != 0:
                raise RuntimeError(
                    'Connection to storage server failed'
                )

    def _get_conlist(self, cli, normalize_path):
        """
        helper method to get conList parameter for connectStorageServer and
        disconnectStorageServer
        :param cli a vscli instance
        :param normalize_path True to force path normalization
        """
        conList = None
        storageType = None
        if self._domain_type in (
                constants.DOMAIN_TYPE_NFS3,
                constants.DOMAIN_TYPE_NFS4,
                constants.DOMAIN_TYPE_GLUSTERFS,
        ):
            conList, storageType = self._get_conlist_nfs_gluster()
            if normalize_path:
                path = self._fix_filebased_connection_path()
                conList[0]['connection'] = path
                self._validate_pre_connected_path(cli, path)
        elif self._domain_type == constants.DOMAIN_TYPE_ISCSI:
            conList, storageType = self._get_conlist_iscsi()
        elif self._domain_type == constants.DOMAIN_TYPE_FC:
            conList, storageType = self._get_conlist_fc()
        else:
            self._log.error(
                "Storage type not supported: '%s'" % self._domain_type
            )
            raise RuntimeError(
                "Storage type not supported: '%s'" % self._domain_type
            )
        if self._mnt_options and conList:
            conList[0]['mnt_options'] = self._mnt_options
        return conList, storageType

    def validate_storage_server(self):
        """
        Checks the hosted-engine storage domain availability
        :return: True if available, False otherwise
        """
        self._log.info("Validating storage server")
        cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )
        status = cli.repoStats(domains=[self._sdUUID])
        try:
            valid = status[self._sdUUID]['valid']
            delay = float(status[self._sdUUID]['delay'])
            if valid and delay <= constants.LOOP_DELAY:
                return True
        except Exception:
            self._log.warn("Hosted-engine storage domain is in invalid state")
        return False

    def connect_storage_server(self):
        """
        Connect the hosted-engine domain storage server
        """
        self._log.info("Connecting storage server")
        cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )
        conList, storageType = self._get_conlist(cli, normalize_path=True)
        if conList:
            self._log.info("Connecting storage server")
            status = cli.connectStorageServer(
                storagepoolID=self._spUUID,
                domainType=storageType,
                connectionParams=conList,
            )
            self._check_connection(status)
        self._log.info("Refreshing the storage domain")
        # calling getStorageDomainStats has the side effect of
        # causing a Storage Domain refresh including
        # all its tree under /rhev/data-center/...
        cli.getStorageDomainStats(self._sdUUID)

    def disconnect_storage_server(self):
        """
        Disconnect the hosted-engine domain storage server
        """
        self._log.info("Disconnecting storage server")
        cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )
        # normalize_path=False since we want to be sure we really disconnect
        # from where we were connected also if its path was wrong
        conList, storageType = self._get_conlist(cli, normalize_path=False)
        if conList:
            status = cli.disconnectStorageServer(
                storagepoolID=self._spUUID,
                domainType=storageType,
                connectionParams=conList,
            )
            self._log.debug(status)
            if status['status']['code'] != 0:
                raise RuntimeError(
                    (
                        'Disconnection to storage server failed, unable '
                        'to recover: {message} - Please try rebooting the '
                        'host to reach a consistent status'
                    ).format(
                        message=status['status']['message']
                    )
                )
