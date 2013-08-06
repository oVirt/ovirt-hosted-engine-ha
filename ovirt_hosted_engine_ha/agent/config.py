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

from . import constants

# constants for hosted-engine.conf options
ENGINE = 'engine'
HOST_ID = 'host_id'
STORAGE_DIR = 'storage_dir'  # FIXME actual value
GATEWAY_ADDR = 'gateway'
ENGINE_FQDN = 'fqdn'
VDSM_SSL = 'vdsm_use_ssl'

# constants for vm.conf options
VM = 'vm'
BRIDGE_NAME = 'bridge'
VM_UUID = 'vmId'


class Config(object):
    def __init__(self):
        self._config = {}
        self._files = {
            ENGINE: constants.ENGINE_SETUP_CONF_FILE,
            VM: constants.VM_CONF_FILE}

        self.load()

    def load(self):
        conf = {}

        for type, fname in self._files.iteritems():
            with open(fname, 'r') as f:
                for line in f:
                    tokens = line.split('=', 1)
                    if len(tokens) > 1:
                        conf[tokens[0]] = tokens[1].strip()
            self._config[type] = conf

    def get(self, type, key):
        try:
            return self._config[type][key]
        except KeyError:
            unknown = "unknown (type={0})".format(type)
            raise Exception("Configuration value not found: file={0}, key={1}"
                            .format(self._files.get(type, unknown), key))
