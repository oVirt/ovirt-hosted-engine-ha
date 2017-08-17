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

"""
Custom exceptions
"""


class DisconnectionError(Exception):
    pass


class BrokerConnectionError(Exception):
    pass


class RequestError(Exception):
    pass


class DetailedError(Exception):
    """
    Allows testing for detailed conditions while preserving clean str(e)
    """
    def __init__(self, msg, detail):
        Exception.__init__(self, msg)
        self.detail = detail


class MetadataError(Exception):
    pass


class FatalMetadataError(Exception):
    pass


class SanlockInitializationError(Exception):
    pass


class BrokerInitializationError(Exception):
    pass


class DuplicateStorageConnectionException(Exception):
    """
    This exception is raised when a potentially duplicate storage connection
    has been detected on a wrong path: see rhbz#1300749

    The agent should exits disconnecting the storage server.
    Systemd will restart the agent if needed.
    """


class StorageDisconnectedError(Exception):
    """
    This exceptions is raised when storage domain is in invalid state.

    The agent should exit.
    Systemd will restart the agent if needed.
    """
