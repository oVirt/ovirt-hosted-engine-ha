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
import os.path
import time

from . import constants

from ovirt_hosted_engine_ha.lib import heconflib
from ovirt_hosted_engine_ha.lib import monotonic
from ovirt_hosted_engine_ha.lib.ovf import ovf_store
from ovirt_hosted_engine_ha.lib.ovf import ovf2VmParams


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
MNT_OPTIONS = 'mnt_options'
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
        self.vm_conf_refresh_time = 0
        self.vm_conf_refresh_time_epoch = 0

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

        self._shared_storage_files = {
            VM: constants.HEConfFiles.HECONFD_VM_CONF
        }

        self._load_config_files(Config.static_files)

        if CONF_FILE in self._config[ENGINE]:
            self._dynamic_files.update({
                VM: self._config[ENGINE][CONF_FILE]
            })

    def _load_single_conf_file(self, fname, config_type):
        """
        loads a single configuration file and writes its content
        to the config dictionary.
        :param fname: the file name to load
        :param config_type: the type of the file name
        """
        conf = {}

        try:
            with open(fname, 'r') as f:
                for line in f:
                    tokens = line.split('=', 1)
                    if len(tokens) > 1:
                        conf[tokens[0].strip()] = tokens[1].strip()
        except (OSError, IOError) as ex:
            log = self._logger

            if config_type in self._dynamic_files \
                    or config_type in self._shared_storage_files:
                level = log.debug
            else:
                level = log.error

            level("Configuration file '%s' not available [%s]", fname, ex)

        self._config.setdefault(config_type, {})
        self._config[config_type].update(conf)

    def _get_config_volume_path(self):
        domain_type = self._config[ENGINE][DOMAIN_TYPE]
        sdUUID = self._config[ENGINE][SD_UUID]
        try:
            conf_vol_uuid = self._config[ENGINE][CONF_VOLUME_UUID]
            conf_img_uuid = self._config[ENGINE][CONF_IMAGE_UUID]
        except (KeyError, ValueError):
            if self._logger:
                self._logger.debug("Configuration image doesn't exist")
            return None

        volumepath = heconflib.get_volume_path(
            domain_type,
            sdUUID,
            conf_img_uuid,
            conf_vol_uuid
        )
        return volumepath

    def _load_config_files(self, conf_files):
        """
        loads the configuration files to the config dictionary.
        :param conf_files: the files to load
        """
        for config_type, fname in conf_files.iteritems():
            self._load_single_conf_file(fname, config_type)

    @property
    def _files(self):
        all_files = {}
        all_files.update(self.static_files)
        all_files.update(self._dynamic_files)
        return all_files

    def get(self, type, key, raise_on_none=False):
        if type in self._dynamic_files.keys():
            self._load_config_files(dict([(type, self._dynamic_files[type])]))

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

            # self._load_config_files() can re-open the exclusively-locked
            # file because it's being called from the same process as the
            # lock holder
            self._load_config_files(dict([(type, self._dynamic_files[type])]))
            self._config[type][key] = str(value)

            text = ''
            for k, v in self._config[type].iteritems():
                text += '{k}={v}\n'.format(k=k, v=v)

            f.write(text)
            f.truncate()

    def refresh_local_conf_file(self, config_type):
        """
        refreshes the local copy of the specified configuration file
        :param config_type: the file to reload
        :return: false if there was an error retrieving the file,
                 true if the file was retrieved and updated.
        """
        content = self._get_file_content_from_shared_storage(
            config_type
        )
        if not content:
            if self._logger:
                self._logger.debug(
                    "unable to get a valid content, "
                    "failing back to conf file from a previous release"
                )
            return False

        localcopy_filename = self._dynamic_files[config_type]
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
            target.truncate()

        return True

    def _get_vm_conf_content_from_ovf_store(self):
        if self._logger:
            self._logger.info(
                "Trying to get a fresher copy of vm configuration "
                "from the OVF_STORE"
            )

        ovfs = ovf_store.OVFStore()
        scan = False

        try:
            scan = ovfs.scan()
        except (EnvironmentError, Exception) as err:
            self._logger.error(
                "Failed scanning for OVF_STORE due to %s",
                err
            )

        if scan:
            heovf = ovfs.getEngineVMOVF()
            if heovf is not None:
                self._logger.info(
                    "Found an OVF for HE VM, "
                    "trying to convert"
                )
                conf = ovf2VmParams.confFromOvf(heovf)
                if conf is not None:
                    self._logger.info('Got vm.conf from OVF_STORE')
                    return conf
                else:
                    self._logger.error(
                        'Failed converting vm.conf from the VM OVF, '
                        'falling back to initial vm.conf'
                    )
            else:
                self._logger.error(
                    'Failed extracting VM OVF from the OVF_STORE '
                    'volume, falling back to initial vm.conf'
                )
        else:
            self._logger.error(
                'Unable to identify the OVF_STORE volume, '
                'falling back to initial vm.conf. Please '
                'ensure you already added your first data '
                'domain for regular VMs'
            )
        return None

    def _get_file_content_from_shared_storage(
            self,
            config_type
    ):
        config_volume_path = self._get_config_volume_path()
        if not config_volume_path:
            return None

        archive_fname = self._shared_storage_files[config_type]
        if self._logger:
            self._logger.debug(
                "Reading '{archive_fname}' from '{source}'".format(
                    archive_fname=archive_fname,
                    source=config_volume_path,
                )
            )
        if heconflib.validateConfImage(self._logger, config_volume_path):
            content = heconflib.extractConfFile(
                self._logger,
                config_volume_path,
                archive_fname,
            )
        return content

    def refresh_vm_conf(self):
        if self._logger:
            self._logger.info(
                "Reloading vm.conf from the shared storage domain"
            )

        content = self._get_vm_conf_content_from_ovf_store()
        if not content:
            if self.refresh_local_conf_file(VM):
                mtime = monotonic.time()
                self.vm_conf_refresh_time = int(mtime)
                self.vm_conf_refresh_time_epoch = int(time.time() - mtime)

    @classmethod
    def static_files_exist(cls):
        """
        Check for the existence of static files and return False if any
        of them are not found or are empty.
        """
        for fname in cls.static_files.values():
            if not os.path.isfile(fname) or os.path.getsize(fname) == 0:
                return False
        return True
