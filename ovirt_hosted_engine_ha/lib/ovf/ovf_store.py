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

import json
import logging


logger = logging.getLogger(__name__)


class OVFStore(object):

    def __init__(self):
        from ovirt_hosted_engine_ha.env import config
        self._log = logging.getLogger("%s.OVFStore" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())
        self._config = config.Config(logger=self._log)

        self._type = self._config.get(config.ENGINE, config.DOMAIN_TYPE)
        self._spUUID = self._config.get(config.ENGINE, config.SP_UUID)
        self._sdUUID = self._config.get(config.ENGINE, config.SD_UUID)
        self._conf_vol_uuid = self._config.get(
            config.ENGINE,
            config.CONF_VOLUME_UUID
        )
        self._conf_img_uuid = self._config.get(
            config.ENGINE,
            config.CONF_IMAGE_UUID
        )
        self._HEVMID = self._config.get(config.ENGINE, config.HEVMID)

        self._ovf_store_imgUUID = None
        self._ovf_store_volUUID = None

    def scan(self):
        self._ovf_store_imgUUID = None
        self._ovf_store_volUUID = None
        _cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )

        imgs = image.Image(self._type, self._sdUUID)
        imageslist = imgs.get_images_list(_cli)

        for img_uuid in imageslist:
            volumeslist = _cli.getVolumesList(
                imageID=img_uuid,
                storagepoolID=self._spUUID,
                storagedomainID=self._sdUUID,
            )
            self._log .debug(volumeslist)
            if volumeslist['status']['code'] != 0:
                raise RuntimeError(volumeslist['status']['message'])
            for vol_uuid in volumeslist['items']:
                volumeinfo = _cli.getVolumeInfo(
                    volumeID=vol_uuid,
                    imageID=img_uuid,
                    storagepoolID=self._spUUID,
                    storagedomainID=self._sdUUID,
                )
                self._log.debug(volumeinfo)
                if volumeinfo['status']['code'] != 0:
                    raise RuntimeError(volumeinfo['status']['message'])
                description = volumeinfo['description']
                if (
                    'Disk Description' in description and
                    description[0] == '{' and
                    description[-1] == '}'
                ):
                    description_dict = json.loads(description)
                    self._log.debug(description_dict)
                    if description_dict['Disk Description'] == 'OVF_STORE':
                        self._ovf_store_imgUUID = img_uuid
                        self._ovf_store_volUUID = vol_uuid
                        self._log.info(
                            'Found OVF_STORE: '
                            'imgUUID:{img}, volUUID:{vol}'.format(
                                img=self._ovf_store_imgUUID,
                                vol=self._ovf_store_volUUID,
                            )
                        )
        if self._ovf_store_imgUUID is None or self._ovf_store_imgUUID is None:
            self._log.warning('Unable to find OVF_STORE')
            return False
        return True

    def getEngineVMOVF(self):
        self._log.info('Extracting Engine VM OVF from the OVF_STORE')
        volumepath = heconflib.get_volume_path(
            self._type,
            self._sdUUID,
            self._ovf_store_imgUUID,
            self._ovf_store_volUUID
        )
        self._log.info('OVF_STORE volume path: %s ' % volumepath)
        filename = self._HEVMID + '.ovf'
        ovf = heconflib.extractConfFile(self._log, volumepath, filename)
        self._log.debug('HEVM OVF: \n%s\n' % ovf)
        if ovf is None:
            self._log.error('Unable to extract HEVM OVF')
        return ovf

# vim: expandtab tabstop=4 shiftwidth=4
