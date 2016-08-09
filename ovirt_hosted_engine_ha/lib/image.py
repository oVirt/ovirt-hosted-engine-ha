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

from ..env import constants
import logging
import os
import glob
from . import log_filter
from ovirt_hosted_engine_ha.lib import util


logger = logging.getLogger(__name__)


class Image(object):

    def __init__(self, stype, sdUUID, extLogger=None):
        if extLogger:
            self._log = extLogger
        else:
            self._log = logging.getLogger("%s.Image" % __name__)
            self._log.addFilter(log_filter.IntermittentFilter())
        # We are not connected to any SP so we must pass a blank UUID
        self._spUUID = constants.BLANK_UUID
        self._sdUUID = sdUUID
        self._storage_type = stype

    def _get_image_path(self):
        """
        Return the base path for images inside the domain
        :param type: storage type
        :param sd_uuid: StorageDomain UUID
        :returns: The local base path for images
        """
        path = constants.SD_MOUNT_PARENT
        if self._storage_type == 'glusterfs':
            path = os.path.join(
                path,
                'glusterSD',
            )
        path = os.path.join(
            path,
            '*',
            self._sdUUID,
            'images',
        )
        volumes = glob.glob(path)
        if not volumes:
            raise RuntimeError(
                'Base path for images not found under {root}'.format(
                    root=constants.SD_MOUNT_PARENT,
                )
            )
        return volumes[0]

    def _my_get_images_list(self):
        """
        VDSM getImagesList on file based devices
        doesn't work when the SD is not connect to
        a storage pool so we have to reimplement it
        explicitly globing on the mount point
        see: https://bugzilla.redhat.com/1274622
        :return the list of available images
        """
        images = set()
        try:
            imageroot = self._get_image_path()
        except RuntimeError:
            return images
        pattern = os.path.join(imageroot, '*-*-*-*-*')
        files = glob.glob(pattern)
        for i in files:
            if os.path.isdir(i):
                images.add(os.path.basename(i))
        return images

    def get_images_list(self, cli):
        """
        It scans for all the available images and volumes on the hosted-engine
        storage domain.
        :param cli a jsonrpcvdscli instance
        :return the list of available images
        """
        result = cli.getImagesList(self._sdUUID)
        self._log.debug('getImagesList: {r}'.format(r=result))
        if result['status']['code'] != 0 or 'items' not in result:
            # VDSM getImagesList doesn't work when the SD is not connect to
            # a storage pool so we have to reimplement it
            # see: https://bugzilla.redhat.com/1274622
            self._log.debug(
                (
                    'VDSM getImagesList failed, '
                    'trying an alternative way: {message}'
                ).format(
                    message=result['status']['message']
                )
            )
            images = self._my_get_images_list()
        else:
            images = result['items']
        return images

    def prepare_images(self):
        """
        It scans for all the available images and volumes on the hosted-engine
        storage domain and for each of them calls prepareImage on VDSM.
        prepareImage will create the needed symlinks and it will activate
        the LV if on block devices.
        """
        self._log.info("Preparing images")
        cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )
        images = self.get_images_list(cli)

        for imgUUID in images:
            vm_vol_uuid_l = cli.getVolumesList(
                imageID=imgUUID,
                storagepoolID=self._spUUID,
                storagedomainID=self._sdUUID,
            )
            self._log.debug(vm_vol_uuid_l)
            if vm_vol_uuid_l['status']['code'] == 0:
                vl = vm_vol_uuid_l['items'] if 'items' in vm_vol_uuid_l else []
                for volUUID in vl:
                    self._log.debug(
                        "Prepare image {storagepoolID} {storagedomainID} "
                        "{imageID} {volumeID}".format(
                            storagepoolID=self._spUUID,
                            storagedomainID=self._sdUUID,
                            imageID=imgUUID,
                            volumeID=volUUID,
                        )
                    )
                    status = cli.prepareImage(
                        storagepoolID=self._spUUID,
                        storagedomainID=self._sdUUID,
                        imageID=imgUUID,
                        volumeID=volUUID,
                    )
                    self._log.debug('Status: {status}'.format(status=status))
                    if status['status']['code'] != 0:
                        self._log.error(
                            (
                                'Error preparing image - storagepoolID: '
                                '{spuuid} - storagedomainID: {sduuid} - '
                                'imageID: {imguuid} - '
                                'volumeID: {voluuid}: {message}'
                            ).format(
                                spuuid=self._spUUID,
                                sduuid=self._sdUUID,
                                imguuid=imgUUID,
                                voluuid=volUUID,
                                message=status['status']['message'],
                            )
                        )
            else:
                self._log.error(
                    'Error fetching volumes list: {msg}'.format(
                        msg=vm_vol_uuid_l['status']['message'],
                    )
                )

    def teardown_images(self):
        """
        It scans for all the available images and volumes on the hosted-engine
        storage domain and for each of them calls teardownImage on VDSM.
        teardownImage will remove the related symlinks and it will deactivate
        the LV if on block devices.
        """
        self._log.info("Teardown images")
        cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )
        images = self.get_images_list(cli)

        for imgUUID in images:
            vm_vol_uuid_l = cli.getVolumesList(
                imageID=imgUUID,
                storagepoolID=self._spUUID,
                storagedomainID=self._sdUUID,
            )
            self._log.debug(vm_vol_uuid_l)
            if vm_vol_uuid_l['status']['code'] == 0:
                vl = vm_vol_uuid_l['items'] if 'items' in vm_vol_uuid_l else []
                for volUUID in vl:
                    self._log.debug(
                        "Teardown image {storagepoolID} {storagedomainID} "
                        "{imageID} {volumeID}".format(
                            storagepoolID=self._spUUID,
                            storagedomainID=self._sdUUID,
                            imageID=imgUUID,
                            volumeID=volUUID,
                        )
                    )
                    status = cli.teardownImage(
                        storagepoolID=self._spUUID,
                        storagedomainID=self._sdUUID,
                        imageID=imgUUID,
                        volumeID=volUUID,
                    )
                    self._log.debug('Status: {status}'.format(status=status))
                    if status['status']['code'] != 0:
                        self._log.error(
                            (
                                'Error teardown image - sp_uuid: {spuuid} - '
                                'sd_uuid: {sduuid} - '
                                'img_uuid: {imguuid} - '
                                'vol_uuid: {voluuid}: {message}'
                            ).format(
                                spuuid=self._spUUID,
                                sduuid=self._sdUUID,
                                imguuid=imgUUID,
                                voluuid=volUUID,
                                message=status['status']['message'],
                            )
                        )
            else:
                self._log.error(
                    'Error fetching volumes list: {msg}'.format(
                        msg=vm_vol_uuid_l['status']['message'],
                    )
                )
