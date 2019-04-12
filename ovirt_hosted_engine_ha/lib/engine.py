#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2014 Red Hat, Inc.
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


class VMState(object):
    UP = "up"

    # This state is used in case:
    # - When the broker starts and the VM is not running locally.
    # - The VM is already running elsewhere and the storage is locked.
    DOWN = "down"

    # The VM is down for any reason other than those mentioned in DOWN state.
    DOWN_UNEXPECTED = "down_unexpected"

    # The VDSM RPC call returns that VM does not exist.
    # This cannot happen with events.
    DOWN_MISSING = "down_missing"


class Health(object):
    GOOD = "good"
    BAD = "bad"
