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
from vdsm import vdscli
from ovirt_hosted_engine_ha.lib import heconflib


logger = logging.getLogger(__name__)


class Image(object):

    def __init__(self):
        # TODO: this is just to avoid an import loop:
        # fix it making all the value from config as
        # constructor parameters and avoid importing config at all
        # This will break the compatibility with ovirt-hosted-engine-setup
        # and it should be addressed also there.
        from ovirt_hosted_engine_ha.env import config
        self._log = logging.getLogger("%s.Image" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())
        self._config = config.Config(logger=self._log)
        self._spUUID = self._config.get(config.ENGINE, config.SP_UUID)
        self._sdUUID = self._config.get(config.ENGINE, config.SD_UUID)
        self._type = self._config.get(config.ENGINE, config.DOMAIN_TYPE)
        self._lockspace_img_id = self._config.get(
            config.ENGINE, config.LOCKSPACE_IMAGE_UUID
        )
        self._lockspace_vol_id = self._config.get(
            config.ENGINE, config.LOCKSPACE_VOLUME_UUID
        )

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
            imageroot = os.path.dirname(os.path.dirname(
                heconflib.get_volume_path(
                    self._type,
                    self._sdUUID,
                    self._lockspace_img_id,
                    self._lockspace_vol_id,
                )
            ))
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
        :param cli a vdscli instance
        :return the list of available images
        """
        result = cli.getImagesList(cli.getImagesList(self._sdUUID))
        if result['status']['code'] != 0:
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
            images = result['imageslist']
        return images

    def prepare_images(self):
        """
        It scans for all the available images and volumes on the hosted-engine
        storage domain and for each of them calls prepareImage on VDSM.
        prepareImage will create the needed symlinks and it will activate
        the LV if on block devices.
        """
        self._log.info("Preparing images")
        cli = vdscli.connect(timeout=constants.VDSCLI_SSL_TIMEOUT)
        images = self.get_images_list(cli)

        for imgUUID in images:
            vm_vol_uuid_list = cli.getVolumesList(
                self._sdUUID,
                self._spUUID,
                imgUUID,
            )
            self._log.debug(vm_vol_uuid_list)
            if vm_vol_uuid_list['status']['code'] == 0:
                for volUUID in vm_vol_uuid_list['uuidlist']:
                    self._log.debug(
                        "Prepare image {spuuid} {sduuid} "
                        "{imguuid} {voluuid}".format(
                            spuuid=self._spUUID,
                            sduuid=self._sdUUID,
                            imguuid=imgUUID,
                            voluuid=volUUID,
                        )
                    )
                    status = cli.prepareImage(
                        self._spUUID,
                        self._sdUUID,
                        imgUUID,
                        volUUID,
                    )
                    self._log.debug('Status: {status}'.format(status=status))
                    if status['status']['code'] != 0:
                        self._log.error(
                            (
                                'Error preparing image - sp_uuid: {spuuid} - '
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
                        msg=vm_vol_uuid_list['status']['message'],
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
        cli = vdscli.connect(timeout=constants.VDSCLI_SSL_TIMEOUT)
        images = self.get_images_list(cli)

        for imgUUID in images:
            vm_vol_uuid_list = cli.getVolumesList(
                self._sdUUID,
                self._spUUID,
                imgUUID,
            )
            self._log.debug(vm_vol_uuid_list)
            if vm_vol_uuid_list['status']['code'] == 0:
                for volUUID in vm_vol_uuid_list['uuidlist']:
                    self._log.debug(
                        "Teardown image {spuuid} {sduuid} "
                        "{imguuid} {voluuid}".format(
                            spuuid=self._spUUID,
                            sduuid=self._sdUUID,
                            imguuid=imgUUID,
                            voluuid=volUUID,
                        )
                    )
                    status = cli.teardownImage(
                        self._spUUID,
                        self._sdUUID,
                        imgUUID,
                        volUUID,
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
                        msg=vm_vol_uuid_list['status']['message'],
                    )
                )
