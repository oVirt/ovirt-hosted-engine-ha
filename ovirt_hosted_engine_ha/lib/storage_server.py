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
import logging
from . import log_filter
from vdsm import vdscli


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
        self._tpgt = None
        try:
            str_tpgt = self._config.get(config.ENGINE, config.ISCSI_TPGT)
            self._tpgt = int(str_tpgt)
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
        if self._domain_type == constants.DOMAIN_TYPE_NFS4:
            storageType = constants.STORAGE_TYPE_NFS
            conDict['protocol_version'] = 4
        if self._self.storageType == constants.DOMAIN_TYPE_GLUSTERFS:
            storageType = constants.STORAGE_TYPE_GLUSTERFS
            conDict['vfs_type'] = 'glusterfs'
        conList = [conDict]
        return conList, storageType

    def _get_conlist_iscsi(self):
        storageType = constants.STORAGE_TYPE_ISCSI
        conDict = {
            'connection': self._storage,
            'iqn': self._iqn,
            'portal': self._portal,
            'user': self._user,
            'password': self._password,
            'id': self._connectionUUID,
            'port': self._port,
        }
        if self._tpgt:
            conDict['tpgt'] = self._tpgt
        conList = [conDict]
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
        for con in status['statuslist']:
            if con['status'] != 0:
                raise RuntimeError(
                    'Connection to storage server failed'
                )

    def connect_storage_server(self):
        self._log.info("Connecting storage server")

        cli = vdscli.connect(timeout=constants.VDSCLI_SSL_TIMEOUT)

        conList = None
        storageType = None
        if self._domain_type in (
                constants.DOMAIN_TYPE_NFS3,
                constants.DOMAIN_TYPE_NFS4,
                constants.DOMAIN_TYPE_GLUSTERFS,
        ):
            conList, storageType = self._get_conlist_nfs_gluster()
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

        if conList:
            self._log.info("Connecting storage server")
            status = cli.connectStorageServer(
                storageType,
                self._spUUID,
                conList
            )
            self._check_connection(status)
