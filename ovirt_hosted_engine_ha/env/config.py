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
from ovirt_hosted_engine_ha.lib import log_filter

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

# constants for vm.conf options
VM = 'vm'
VM_UUID = 'vmId'
MEM_SIZE = 'memSize'

# constants for ha.conf options
HA = 'ha'
LOCAL_MAINTENANCE = 'local_maintenance'

BROKER = 'broker'
HE_CONF = 'he_conf'

LF_OVF_NOT_THERE = 'ovf-not-there'
LF_OVF_EXTRACTION_FAILED = 'ovf-extraction-failed'
LF_OVF_CONVERSION_FAILED = 'ovf-conversion-failed'
LF_OVF_LOG_DELAY = 300


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
            BROKER: constants.NOTIFY_CONF_FILE,
            HE_CONF: constants.ENGINE_SETUP_CONF_FILE
        }

        # This dictionary holds names of config files that are stored in the
        # configuration archive on the shared storage.
        # The key is the config type that can be edited and the value is
        # the file name in the configuration archive.
        # {type -> file name in archive}
        self._shared_storage_files = {
            BROKER: constants.HEConfFiles.HECONFD_BROKER_CONF,
            VM: constants.HEConfFiles.HECONFD_VM_CONF,
            HE_CONF: constants.HEConfFiles.HECONFD_HECONF
        }

        # This dictionary holds methods that will be called when refreshing
        # the config file.
        # If no refresh method is specified the _refresh_local_conf_file
        # will be called.
        self._shared_storage_refresh_method = {
            VM: self.refresh_vm_conf
        }

        self._editable_shared_storage_files = {
            BROKER,
            HE_CONF
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
        in case the file is saved on shared storage the method
        will update the local copy of the file as well.
        :param conf_files: the files to load
        """
        for config_type, fname in conf_files.iteritems():
            if config_type in self._shared_storage_files:
                self.refresh_local_conf_file(config_type)
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

    def get_config_from_shared_storage(self, key, config_type=None):
        final_type = self._determine_final_config_type(key, config_type)
        value = self.get(final_type, key)
        value_and_type = [value, final_type]
        return value_and_type

    def _determine_final_config_type(self, key, config_type=None):
        self._load_config_files(self._dynamic_files)
        final_type = config_type
        if config_type:
            if config_type not in self._editable_shared_storage_files:
                allowed_types = ", ".join(self._editable_shared_storage_files)
                raise Exception("Invalid configuration type {0}, "
                                "supported types are: {1}"
                                .format(config_type, allowed_types)
                                )
        else:
            key_count = 0
            for shared_config_type in self._shared_storage_files:
                shared_config_dict = self._config[shared_config_type]
                for config_key in shared_config_dict:
                    if key == config_key:
                        if key_count > 0:
                            raise Exception("Duplicate key {0}, "
                                            "please specify the key type"
                                            .format(key))
                        final_type = shared_config_type
                        key_count += 1
            if key_count == 0:
                raise KeyError(
                    "Configuration key not found: key={0}".format(key))
        return final_type

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

    def set_config_on_shared_storage(self, key, value, config_type=None):
        """
        Writes 'key=value' to the config file for a file on
        shared storage for 'config_type'.
        If config_type is not provided the type is found based
        on the provided key.
        The method updates the configuration dictionary, the local
        copy of the file, and the copy on the configuration archive
        in shared storage.
        :param key: the key to set
        :param value: the value to set
        :param config_type: the type of the file
        """
        final_type = self._determine_final_config_type(key, config_type)
        if key in self._config[final_type]:
            self._config[final_type][key] = value
        else:
            raise KeyError(
                "Configuration key not found: type={0}, key={1}".format(
                    final_type,
                    key
                )
            )

        content = self._create_new_content_for_file_on_shared_storage(
            final_type,
            key,
            value
        )
        self._update_content_on_shared_conf(final_type, content)
        self._load_config_files(self._dynamic_files)

    def _update_content_on_shared_conf(self, config_type, file_content):
        """
        writes the new content to the specified file.
        :param config_type: the file to update.
        :param file_content: the new content
        """
        file_name = self._shared_storage_files[config_type]
        volumepath = self._get_config_volume_path()
        heconflib.add_file_to_conf_archive(
            self._logger,
            file_name,
            file_content,
            volumepath
        )

    def _create_new_content_for_file_on_shared_storage(
            self,
            config_type,
            key_to_set,
            new_value
    ):
        """
        creates the new content for the specified config_type
        with the new value for the given key.
        this method does not update the file.
        :param config_type: the file to update
        :param key_to_set: the key to set
        :param new_value: the value to set
        :return: the updated content of the file
        """
        text = ''
        file_content = self._get_file_content_from_shared_storage(
            config_type
        )
        for line in file_content.splitlines():
            tokens = line.split('=', 1)
            if len(tokens) > 1:
                key = tokens[0].strip()
                value = tokens[1].strip()
                if key != key_to_set or value == new_value:
                    new_line = line
                else:
                    new_line = '{key}={new_value}'.\
                        format(key=key_to_set, new_value=new_value)
            else:
                new_line = line
            text += '{line}\n'.format(line=new_line)

        return text

    def refresh_local_conf_file(self, config_type):
        """
        refreshes the local copy of the specified configuration file
        :param config_type: the file to reload
        :return: false if there was an error retrieving the file,
                 true if the file was retrieved and updated.
        """
        # Use a dedicated refresh method if configured
        if config_type in self._shared_storage_refresh_method:
            refresh_method = self._shared_storage_refresh_method[config_type]
            result = refresh_method()
            return True if result is None else result

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

        return self._publish_local_conf_file(config_type, content)

    def _publish_local_conf_file(self, config_type, content):
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
            self._logger.debug(
                "Trying to get a fresher copy of vm configuration "
                "from the OVF_STORE"
            )

        ovfs = ovf_store.OVFStore()

        if not ovfs.have_store_info():
            try:
                ovfs.scan()
            except (EnvironmentError, Exception) as err:
                self._logger.error(
                    "Failed scanning for OVF_STORE due to %s",
                    err
                )

        if ovfs.have_store_info():
            heovf = ovfs.getEngineVMOVF()
            if heovf is not None:
                self._logger.debug(
                    "Found an OVF for HE VM, "
                    "trying to convert"
                )
                conf = ovf2VmParams.confFromOvf(heovf)
                if conf is not None:
                    self._logger.debug('Got vm.conf from OVF_STORE')
                    return conf
                else:
                    self._logger.error(
                        'Failed converting vm.conf from the VM OVF, '
                        'falling back to initial vm.conf',
                        extra=log_filter.lf_args(
                            LF_OVF_CONVERSION_FAILED,
                            LF_OVF_LOG_DELAY)
                    )
            else:
                self._logger.error(
                    'Failed extracting VM OVF from the OVF_STORE '
                    'volume, falling back to initial vm.conf',
                    extra=log_filter.lf_args(
                        LF_OVF_EXTRACTION_FAILED,
                        LF_OVF_LOG_DELAY)
                )
                # This error might indicate the OVF location changed
                # and clearing the cache will trigger a rescan
                # next time we access the OVF.
                ovfs.clear_store_info()
        else:
            self._logger.error(
                'Unable to identify the OVF_STORE volume, '
                'falling back to initial vm.conf. Please '
                'ensure you already added your first data '
                'domain for regular VMs',
                extra=log_filter.lf_args(
                    LF_OVF_NOT_THERE,
                    LF_OVF_LOG_DELAY
                )
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
        content = None
        if heconflib.validateConfImage(self._logger, config_volume_path):
            content = heconflib.extractConfFile(
                self._logger,
                config_volume_path,
                archive_fname,
            )
        return content

    def refresh_vm_conf(self):
        header_comment = (
            '# Editing the hosted engine VM is only possible via'
            ' the manager UI\API\n\n'
        )
        if self._logger:
            self._logger.debug(
                "Reloading vm.conf from the shared storage domain"
            )

        content_from_ovf = self._get_vm_conf_content_from_ovf_store()
        if content_from_ovf:
            content = content_from_ovf
        else:
            content = self._get_file_content_from_shared_storage(VM)

        if content:
            content = "%s%s" % (header_comment, content)
            if self._publish_local_conf_file(VM, content):
                mtime = monotonic.time()
                self.vm_conf_refresh_time = int(mtime)
                self.vm_conf_refresh_time_epoch = int(time.time() - mtime)

    def get_all_shared_keys(self, config_type):
        if (config_type and
                config_type not in self._editable_shared_storage_files):
            return None
        config_type_to_keys = {}
        self._load_config_files(self._dynamic_files)
        if config_type:
            config_type_to_keys[config_type] = \
                self._config[config_type].keys()
        else:
            for config_type in self._editable_shared_storage_files:
                config_type_to_keys[config_type] = \
                    self._config[config_type].keys()
        return config_type_to_keys

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
