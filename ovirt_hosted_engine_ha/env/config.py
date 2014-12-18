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

from . import constants

# constants for hosted-engine.conf options
ENGINE = 'engine'
DOMAIN_TYPE = 'domainType'
ENGINE_FQDN = 'fqdn'
GATEWAY_ADDR = 'gateway'
HOST_ID = 'host_id'
SD_UUID = 'sdUUID'
SP_UUID = 'spUUID'
VDSM_SSL = 'vdsm_use_ssl'
BRIDGE_NAME = 'bridge'
METADATA_VOLUME_UUID = 'metadata_volume_UUID'
METADATA_IMAGE_UUID = 'metadata_image_UUID'
LOCKSPACE_VOLUME_UUID = 'lockspace_volume_UUID'
LOCKSPACE_IMAGE_UUID = 'lockspace_image_UUID'


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
        VM: constants.VM_CONF_FILE,
    }
    # Config files in dynamic_files may change at runtime and are re-read
    # whenever configuration values are retrieved from them.
    dynamic_files = {
        HA: constants.HA_AGENT_CONF_FILE,
    }

    def __init__(self):
        self._config = {}
        self._load(Config.static_files)

    def _load(self, files):
        conf = {}

        for type, fname in files.iteritems():
            with open(fname, 'r') as f:
                for line in f:
                    tokens = line.split('=', 1)
                    if len(tokens) > 1:
                        conf[tokens[0]] = tokens[1].strip()
            self._config[type] = conf

    def get(self, type, key, raise_on_none=False):
        if type in Config.dynamic_files.keys():
            self._load(dict([(type, Config.dynamic_files[type])]))

        try:
            val = self._config[type][key]
            if raise_on_none and val in (None, "None", ""):
                raise ValueError(
                    "'{0} can't be '{1}'".format(key, val)
                )
            return val
        except KeyError:
            unknown = "unknown (type={0})".format(type)
            raise Exception("Configuration value not found: file={0}, key={1}"
                            .format(self._files.get(type, unknown), key))

    def set(self, type, key, value):
        """
        Writes 'key=value' to the config file for 'type'.
        Note that this method is not thread safe.
        """
        if type not in Config.dynamic_files:
            raise Exception("Configuration type {0} cannot be updated"
                            .format(type))

        with open(Config.dynamic_files[type], 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)

            # self._load() can re-open the exclusively-locked file because
            # it's being called from the same process as the lock holder
            self._load(dict([(type, Config.dynamic_files[type])]))
            self._config[type][key] = str(value)

            text = ''
            for k, v in self._config[type].iteritems():
                text += '{k}={v}\n'.format(k=k, v=v)

            f.write(text)
            f.truncate()
