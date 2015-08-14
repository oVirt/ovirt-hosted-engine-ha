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


class Image(object):

    def __init__(self):
        self._log = logging.getLogger("%s.Image" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())
        self._config = config.Config(logger=self._log)
        self._spUUID = self._config.get(config.ENGINE, config.SP_UUID)
        self._sdUUID = self._config.get(config.ENGINE, config.SD_UUID)
        self._vm_disk_img_id = self._config.get(
            config.ENGINE, config.VM_DISK_IMG_ID
        )
        # VM disk vol uuid was not in 3.5 conf file but now vdsm requires it
        # reading or fetching it only when needed
        self._metadata_img_id = self._config.get(
            config.ENGINE, config.METADATA_IMAGE_UUID
        )
        self._metadata_vol_id = self._config.get(
            config.ENGINE, config.METADATA_VOLUME_UUID
        )
        self._lockspace_img_id = self._config.get(
            config.ENGINE, config.LOCKSPACE_IMAGE_UUID
        )
        self._lockspace_vol_id = self._config.get(
            config.ENGINE, config.LOCKSPACE_VOLUME_UUID
        )
        # Shared conf volume wasn't present in oVirt 3.5
        self._conf_img_id = None
        self._conf_vol_id = None

    def prepare_images(self):
        # prepareImage to populate /var/run/vdsm/storage
        self._log.info("Preparing images")
        cli = vdscli.connect(timeout=constants.VDSCLI_SSL_TIMEOUT)

        try:
            self._vm_disk_vol_id = self._config.get(
                config.ENGINE,
                config.VM_DISK_VOL_ID
            )
        except (KeyError, ValueError):
            vm_vol_uuid_list = cli.getVolumesList(
                self._sdUUID,
                self._spUUID,
                self._vm_disk_img_id,
            )
            if vm_vol_uuid_list['status']['code'] == 0:
                self._vm_disk_vol_id = vm_vol_uuid_list['uuidlist'][0]

        try:
            self._conf_img_id = self._config.get(
                config.ENGINE, config.CONF_IMAGE_UUID
            )
            self._conf_vol_id = self._config.get(
                config.ENGINE, config.CONF_VOLUME_UUID
            )
        except (KeyError, ValueError):
            self._log.debug("Configuration image doesn't exist")

        for imgUUID, volUUID in [
            [
                self._vm_disk_img_id,
                self._vm_disk_vol_id
            ],
            [
                self._metadata_img_id,
                self._metadata_vol_id
            ],
            [
                self._lockspace_img_id,
                self._lockspace_vol_id
            ],
            [
                self._conf_img_id,
                self._conf_vol_id
            ],
        ]:
            if imgUUID and volUUID:
                self._log.debug(
                    "Prepare image {spuuid} {sduuid} "
                    "{imguuid} {voluuid}".format(
                        spuuid=self._spUUID,
                        sduuid=self._sdUUID,
                        imguuid=imgUUID,
                        voluuid=volUUID,
                    )
                )
                cli.prepareImage(
                    self._spUUID,
                    self._sdUUID,
                    imgUUID,
                    volUUID,
                )
