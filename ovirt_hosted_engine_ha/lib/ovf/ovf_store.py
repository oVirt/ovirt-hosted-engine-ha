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

from ovirt_hosted_engine_ha.env import constants
from ovirt_hosted_engine_ha.lib import heconflib
from ovirt_hosted_engine_ha.lib import image
from ovirt_hosted_engine_ha.lib import log_filter
from ovirt_hosted_engine_ha.lib import util

from vdsm.client import ServerError

import json
import logging


logger = logging.getLogger(__name__)

LF_OVF_STORE_NOT_FOUND = 'ovf-store-not-found'
LF_OVF_STORE_PATH = 'ovf-store-path'
LF_EXTRACTION_FAILED = 'ovf-extraction-failed'
LF_OVF_LOG_DELAY = 300

class OVFStore(object):
    """
    OVF location data cache
    to avoid repeated rescans
    """
    _ovf_store_path = None

    def __init__(self):
        from ovirt_hosted_engine_ha.env import config
        from ovirt_hosted_engine_ha.env import config_constants as const

        self._log = logging.getLogger("%s.OVFStore" % __name__)
        self._log.addFilter(log_filter.get_intermittent_filter())
        self._config = config.Config(logger=self._log)

        self._type = self._config.get(config.ENGINE, const.DOMAIN_TYPE)
        self._spUUID = self._config.get(config.ENGINE, const.SP_UUID)
        self._sdUUID = self._config.get(config.ENGINE, const.SD_UUID)
        self._conf_vol_uuid = self._config.get(
            config.ENGINE,
            const.CONF_VOLUME_UUID
        )
        self._conf_img_uuid = self._config.get(
            config.ENGINE,
            const.CONF_IMAGE_UUID
        )
        self._HEVMID = self._config.get(config.ENGINE, const.HEVMID)

    def have_store_info(self):
        return OVFStore._ovf_store_path

    def clear_store_info(self):
        OVFStore._ovf_store_path = None

    def scan(self):
        self.clear_store_info()

        cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )

        imgs = image.Image(self._type, self._sdUUID)
        imageslist = imgs.get_images_list(cli)

        for img_uuid in imageslist:
            try:
                volumeslist = cli.StorageDomain.getVolumes(
                    imageID=img_uuid,
                    storagepoolID=self._spUUID,
                    storagedomainID=self._sdUUID,
                )
                self._log .debug(volumeslist)
            except ServerError as e:
                raise RuntimeError(str(e))

            for vol_uuid in volumeslist:
                try:
                    volumeinfo = cli.Volume.getInfo(
                        volumeID=vol_uuid,
                        imageID=img_uuid,
                        storagepoolID=self._spUUID,
                        storagedomainID=self._sdUUID,
                    )
                    self._log.debug(volumeinfo)
                except ServerError as e:
                    raise RuntimeError(str(e))

                description = volumeinfo['description']
                if (
                    'Disk Description' in description and
                    description[0] == '{' and
                    description[-1] == '}'
                ):
                    description_dict = json.loads(description)
                    self._log.debug(description_dict)
                    if description_dict['Disk Description'] == 'OVF_STORE':
                        self._log.info(
                            'Found OVF_STORE: '
                            'imgUUID:{img}, volUUID:{vol}'.format(
                                img=img_uuid,
                                vol=vol_uuid,
                            )
                        )

                        # Prepare symlinks for the OVF store
                        try:
                            image_info = cli.Image.prepare(
                                storagepoolID=self._spUUID,
                                storagedomainID=self._sdUUID,
                                imageID=img_uuid,
                                volumeID=vol_uuid
                            )
                            OVFStore._ovf_store_path = image_info["path"]
                        except ServerError as e:
                            raise RuntimeError(str(e))

        if self._ovf_store_path is None:
            self._log.warning('Unable to find OVF_STORE',
                              extra=log_filter.lf_args(
                                  LF_OVF_STORE_NOT_FOUND,
                                  LF_OVF_LOG_DELAY
                              ))
            return False
        return True

    def getEngineVMOVF(self):
        self._log.debug('Extracting Engine VM OVF from the OVF_STORE')
        volumepath = OVFStore._ovf_store_path
        self._log.info('OVF_STORE volume path: %s ' % volumepath,
                       extra=log_filter.lf_args(
                           LF_OVF_STORE_PATH,
                           LF_OVF_LOG_DELAY
                       ))
        filename = self._HEVMID + '.ovf'
        ovf = heconflib.extractConfFile(self._log, volumepath, filename)
        self._log.debug('HEVM OVF: \n%s\n' % ovf)
        if ovf is None:
            self._log.error('Unable to extract HEVM OVF',
                            extra=log_filter.lf_args(
                                LF_EXTRACTION_FAILED,
                                LF_OVF_LOG_DELAY
                            ))
        return ovf

# vim: expandtab tabstop=4 shiftwidth=4
