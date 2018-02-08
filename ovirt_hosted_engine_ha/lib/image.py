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

from vdsm.client import ServerError


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
        :param cli a clinet instance
        :return the list of available images
        """
        try:
            images = cli.StorageDomain.getImages(storagedomainID=self._sdUUID)
            self._log.debug('getImagesList: {r}'.format(r=images))
        except ServerError as e:
            # VDSM getImagesList doesn't work when the SD is not connect to
            # a storage pool so we have to reimplement it
            # see: https://bugzilla.redhat.com/1274622
            self._log.debug(
                (
                    'VDSM getImagesList failed, '
                    'trying an alternative way: {message}'
                ).format(message=str(e))
            )
            images = self._my_get_images_list()

        return images
