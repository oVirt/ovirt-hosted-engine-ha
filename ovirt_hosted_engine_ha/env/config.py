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

import logging

from ovirt_hosted_engine_ha.env.config_ini import SharedIniFile
from ovirt_hosted_engine_ha.lib import util
from .config_file import ConfigFile
from .config_ovf import OvfConfigFile
from .config_shared import SharedConfigFile
from . import constants
from .config_constants import ENGINE, CONF_FILE, BROKER, HE_CONF, VM, HA,\
    LEGACY_VM_CONF, ENGINE_OPTIONAL_KEYS, LOCAL_MAINTENANCE,\
    LOCAL_MAINTENANCE_MANUAL


class Config(object):
    def __init__(self, logger=None):
        if logger is None:
            self._logger = logging.getLogger(__name__)
        else:
            self._logger = logger

        # Initialize the primary configuration
        sd_config = ConfigFile(ENGINE, constants.ENGINE_SETUP_CONF_FILE,
                               mandatory=True, logger=self._logger,
                               writable=True,
                               optional_keys=ENGINE_OPTIONAL_KEYS)
        sd_config.download()
        sd_config.load()

        # Prepare a configuration source for getting the VM details
        vm_conf_path = sd_config.get(CONF_FILE, constants.LOCAL_VM_CONF_PATH)
        legacy_vm_conf = SharedConfigFile(
            LEGACY_VM_CONF,
            constants.LOCAL_VM_CONF_PATH_FALLBACK,
            remote_path="vm.conf",
            sd_config=sd_config,
            writable=False,
            logger=self._logger)

        self._vm_config = OvfConfigFile(
            VM, vm_conf_path,
            sd_config=sd_config,
            legacy_vm_conf=legacy_vm_conf,
            logger=self._logger)

        # List of all "public" configuration sources
        self.config_files = [
            sd_config,
            SharedIniFile(BROKER, constants.NOTIFY_CONF_FILE,
                          sd_config=sd_config,
                          writable=True,
                          logger=self._logger),
            SharedConfigFile(HE_CONF,
                             constants.CACHED_ENGINE_SETUP_CONF_FILE,
                             sd_config=sd_config,
                             writable=True, logger=self._logger,
                             optional_keys=ENGINE_OPTIONAL_KEYS),
            ConfigFile(HA, constants.HA_AGENT_CONF_FILE,
                       writable=True, logger=self._logger),
            self._vm_config
        ]

        # Prepare a config ID -> config index map
        self._config_map = {v.id: v for v in self.config_files}

    def get(self, type, key, raise_on_none=False):
        try:
            val = self._config_map[type][key]
            if raise_on_none and val in (None, "None", ""):
                raise ValueError(
                    "'{0} can't be '{1}'".format(key, val)
                )
            return val
        except KeyError:
            raise KeyError(
                "Configuration value not found: file={0}, key={1}".format(
                    self._config_map[type].path,
                    key
                )
            )

    def get_config_from_shared_storage(self, key, config_type=None):
        self._refresh_config()
        final_type = self._determine_final_config_type(key, config_type)
        value = final_type.get(key)
        value_and_type = [value, final_type.id]
        return value_and_type

    def _determine_final_config_type(self, key, config_type=None):
        cfgs = [c for c in self.config_files
                if key in c
                if not config_type or c.id == config_type]

        if len(cfgs) == 0:
            raise KeyError(
                "Configuration key not found: key={0}".format(key))
        elif len(cfgs) > 1:
            key_types = [c.id for c in cfgs]
            raise Exception("Duplicate key {0}, "
                            "please specify the key type. "
                            "Available types are: {1}"
                            .format(key, key_types))
        elif cfgs[0].readonly:
            allowed_types = [c.id for c in self.config_files]
            raise Exception("Invalid configuration type {0}, "
                            "supported types are: {1}"
                            .format(config_type, allowed_types))
        else:
            return cfgs[0]

    def set(self, type, key, value):
        """
        Writes 'key=value' to the config file for 'type'.
        Note that this method is not thread safe.
        """
        cfg = self._config_map[type]
        if cfg.readonly:
            raise Exception("Configuration type {0} cannot be updated"
                            .format(type))
        cfg.set_and_write(key, value)

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
        self._refresh_config()
        final_type = self._determine_final_config_type(key, config_type)
        if key in final_type:
            final_type.set_and_write(key, value)
        else:
            raise KeyError(
                "Configuration key not found: type={0}, key={1}".format(
                    final_type,
                    key
                )
            )

    def get_all_shared_keys(self, config_type):
        if (config_type and
                self._config_map[config_type].readonly):
            return None

        if config_type:
            cfg = self._config_map[config_type]
            cfg.download()
            cfg.load()
            return {config_type: sorted(set(
                list(cfg.keys()) + cfg.optional_keys()))}
        else:
            self._refresh_config()
            return {
                cfg.id: sorted(set(list(cfg.keys()) + cfg.optional_keys()))
                for cfg in self.config_files
                if not cfg.readonly
            }

    def _refresh_config(self):
        for c in self.config_files:
            if c.readonly:
                continue
            c.download()
            c.load()

    @classmethod
    def static_files_exist(cls):
        """
        Check for the existence of static files and return False if any
        of them are not found or are empty.
        """
        cfg_inst = cls()
        return all(
            [cfg.present() for cfg in cfg_inst.config_files if cfg.mandatory]
        )

    def refresh_vm_conf(self):
        self._vm_config.download()
        self._vm_config.load()
        return self._vm_config.path

    @property
    def vm_conf_refresh_time(self):
        return self._vm_config.vm_conf_refresh_time

    @property
    def vm_conf_refresh_time_epoch(self):
        return self._vm_config.vm_conf_refresh_time_epoch

    def refresh_local_conf_file(self, cfg):
        self._config_map[cfg].download()
        self._config_map[cfg].load()
        return self._config_map[cfg].path

    # Copied and amended from collect_stats in the agent. TODO: Unite?
    def get_local_maintenance(self):
        self.refresh_local_conf_file(HA)
        manual_maintenance = util.to_bool(
            self.get(HA, LOCAL_MAINTENANCE_MANUAL)
        )
        local_maintenance = util.to_bool(self.get(HA, LOCAL_MAINTENANCE))
        return manual_maintenance or local_maintenance
