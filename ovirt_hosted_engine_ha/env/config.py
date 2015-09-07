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

import fcntl
import logging

from . import constants

from ovirt_hosted_engine_ha.lib import heconflib


# constants for hosted-engine.conf options
ENGINE = 'engine'
DOMAIN_TYPE = 'domainType'
ENGINE_FQDN = 'fqdn'
CONFIGURED = 'configured'
GATEWAY_ADDR = 'gateway'
HOST_ID = 'host_id'
SD_UUID = 'sdUUID'
SP_UUID = 'spUUID'
VDSM_SSL = 'vdsm_use_ssl'
BRIDGE_NAME = 'bridge'
VM_DISK_IMG_ID = 'vm_disk_id'
VM_DISK_VOL_ID = 'vm_disk_vol_id'
METADATA_VOLUME_UUID = 'metadata_volume_UUID'
METADATA_IMAGE_UUID = 'metadata_image_UUID'
LOCKSPACE_VOLUME_UUID = 'lockspace_volume_UUID'
LOCKSPACE_IMAGE_UUID = 'lockspace_image_UUID'
CONF_VOLUME_UUID = 'conf_volume_UUID'
CONF_IMAGE_UUID = 'conf_image_UUID'
CONF_FILE = 'conf'
HEVMID = 'vmid'
STORAGE = 'storage'
CONNECTIONUUID = 'connectionUUID'
# The following are used only for iSCSI storage
ISCSI_IQN = 'iqn'
ISCSI_PORTAL = 'portal'
ISCSI_USER = 'user'
ISCSI_PASSWORD = 'password'
ISCSI_PORT = 'port'
ISCSI_TPGT = 'tpgt'

# constants for vm.conf options
VM = 'vm'
VM_UUID = 'vmId'
MEM_SIZE = 'memSize'

# constants for ha.conf options
HA = 'ha'
LOCAL_MAINTENANCE = 'local_maintenance'


class Config(object):
    static_files = {
        ENGINE: constants.ENGINE_SETUP_CONF_FILE,
    }

    def __init__(self, logger=None):
        if logger is None:
            self._logger = logging.getLogger(__name__)
        else:
            # TODO use logger.getChild() when Python 2.6 support is dropped
            self._logger = logging.getLogger(logger.name + ".config")

        self._config = {
            ENGINE: {
                HOST_ID: None,
                CONFIGURED: None
            }
        }

        # Config files in dynamic_files may change at runtime and are re-read
        # whenever configuration values are retrieved from them.
        self._dynamic_files = {
            HA: constants.HA_AGENT_CONF_FILE,
        }

        self._load(Config.static_files)

        if CONF_FILE in self._config[ENGINE]:
            self._dynamic_files.update({
                VM: self._config[ENGINE][CONF_FILE]
            })

    def _load(self, files):
        for type, fname in files.iteritems():
            conf = {}

            try:
                with open(fname, 'r') as f:
                    for line in f:
                        tokens = line.split('=', 1)
                        if len(tokens) > 1:
                            conf[tokens[0]] = tokens[1].strip()
            except (OSError, IOError) as ex:
                log = self._logger

                if type in self._dynamic_files:
                    level = log.debug
                else:
                    level = log.error

                level("Configuration file '%s' not available [%s]", fname, ex)

            self._config.setdefault(type, {})
            self._config[type].update(conf)

    @property
    def _files(self):
        all_files = {}
        all_files.update(self.static_files)
        all_files.update(self._dynamic_files)
        return all_files

    def get(self, type, key, raise_on_none=False):
        if type in self._dynamic_files.keys():
            self._load(dict([(type, self._dynamic_files[type])]))

        try:
            val = self._config[type][key]
            if raise_on_none and val in (None, "None", ""):
                raise ValueError(
                    "'{0} can't be '{1}'".format(key, val)
                )
            return val
        except KeyError:
            fname = "unknown (type={0})".format(type)
            if type in Config.static_files.keys():
                fname = Config.static_files[type]
            elif type in self._dynamic_files.keys():
                fname = self._dynamic_files[type]
            raise KeyError(
                "Configuration value not found: file={0}, key={1}".format(
                    fname,
                    key
                )
            )

    def set(self, type, key, value):
        """
        Writes 'key=value' to the config file for 'type'.
        Note that this method is not thread safe.
        """
        if type not in self._dynamic_files:
            raise Exception("Configuration type {0} cannot be updated"
                            .format(type))

        with open(self._dynamic_files[type], 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)

            # self._load() can re-open the exclusively-locked file because
            # it's being called from the same process as the lock holder
            self._load(dict([(type, self._dynamic_files[type])]))
            self._config[type][key] = str(value)

            text = ''
            for k, v in self._config[type].iteritems():
                text += '{k}={v}\n'.format(k=k, v=v)

            f.write(text)
            f.truncate()

    def refresh_local_conf_file(self, localcopy_filename, archive_fname):
        domain_type = self.get(ENGINE, DOMAIN_TYPE)
        sd_uuid = self.get(ENGINE, SD_UUID)
        conf_img_id = None
        conf_vol_id = None
        try:
            conf_img_id = self.get(ENGINE, CONF_IMAGE_UUID)
            conf_vol_id = self.get(ENGINE, CONF_VOLUME_UUID)
        except (KeyError, ValueError):
            if self._logger:
                self._logger.debug("Configuration image doesn't exist")
            pass

        if not (conf_img_id and conf_vol_id):
            if self._logger:
                self._logger.debug(
                    "Failing back to conf file from a previous release"
                )
            return False
        else:
            source = heconflib.get_volume_path(
                domain_type,
                sd_uuid,
                conf_img_id,
                conf_vol_id,
            )
            if self._logger:
                self._logger.debug(
                    "Reading '{archive_fname}' from '{source}'".format(
                        archive_fname=archive_fname,
                        source=source,
                    )
                )

            if heconflib.validateConfImage(self._logger, source):
                content = heconflib.extractConfFile(
                    self._logger,
                    source,
                    archive_fname,
                )
                if self._logger:
                    self._logger.debug(
                        "Writing to '{target}'".format(
                            target=localcopy_filename,
                        )
                    )
                with open(localcopy_filename, 'w') as target:
                    target.write(content)
                if self._logger:
                    self._logger.debug(
                        "local conf file was correctly written"
                    )
                return True
            if self._logger:
                self._logger.debug(
                    "failed trying to write local conf file"
                )
            return False
