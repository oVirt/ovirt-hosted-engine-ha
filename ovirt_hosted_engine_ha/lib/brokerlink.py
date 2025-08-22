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
import xmlrpc.client as xmlrpc_client
from xmlrpc.client import Marshaller

from ..env import constants
from ..lib.exceptions import RequestError
from ..lib import unixrpc


def big_int_marshaller(m, value, writer):
    if value >= 2 ** 31 or value <= -2 ** 31:
        writer("<value><i8>%d</i8></value>" % value)
    else:
        writer("<value><int>%d</int></value>" % value)


def enable_i8():
    """
    Enable i8 extension
    Python 2.7 knows how to read it, but sending needs to be configured
    """
    Marshaller.dispatch[int] = big_int_marshaller


enable_i8()


class NotifyEvents(object):
    STATE_TRANSITION = "state_transition"


class BrokerLink(object):
    def __init__(self, timeout=None):
        self._log = logging.getLogger("%s.BrokerLink" % __name__)
        self._proxy = unixrpc.UnixXmlRpcClient(
            constants.BROKER_SOCKET_FILE,
            timeout=timeout,
        )

    def notify(self, event_type, detail, **options):
        try:
            response = self._proxy.notify(event_type, detail, options)
        except Exception as e:
            raise RequestError(("Failed to send notification about {0}, "
                                "detail {1}, options {2}: {3}")
                               .format(event_type, detail, options, e))

        self._log.info("Success, was notification of "
                       "%s (%s) sent? %s", event_type, detail, response)
        return response

    def start_monitor(self, type, options):
        """
        Starts a monitor of the specified type in the ha broker using the
        given options dictionary, returning an id on success.
        """
        self._log.info("Starting monitor {0}, options {1}"
                       .format(type, options))

        try:
            response = self._proxy.start_monitor(type, options)
        except Exception as e:
            raise RequestError(
                (
                    "brokerlink - failed to start monitor via "
                    "ovirt-ha-broker: {e}, [monitor: '{t}', options: {o}]"
                ).format(t=type, o=options, e=e)
            )

        self._log.info("Success, id %s", response)
        return response

    def get_monitor_status(self, id):
        try:
            response = self._proxy.status_monitor(id)
        except Exception as e:
            self._log.error("Exception getting monitor status: %s", str(e))
            raise RequestError(
                (
                    "brokerlink - failed to get monitor status from "
                    "ovirt-ha-broker: {e}"
                ).format(e=e)
            )
        self._log.debug("Success, status %s", response)
        return response

    def put_stats_on_storage(self, host_id, data):
        """
        Puts data on the shared storage according to the parameters.
        Data should be passed in as a string.
        """
        self._log.debug("Storing blocks on storage")
        # broker expects blocks in hex format
        self._proxy.put_stats(host_id, xmlrpc_client.Binary(data.encode()))

    def put_hosts_state_on_storage(self, host_id, alive_hosts):
        """
        Broker expects list of alive hosts in format:
        <host_id>|<host_id>
        The broker adds it's own monotonic timestamp before saving it
        """

        # since we're reporting it we are alive ;)
        _alive_hosts = [host_id] + alive_hosts

        self._log.debug("Updating live hosts list")
        self._proxy.push_hosts_state(_alive_hosts)

    def is_host_alive(self, host_id):
        host_list = self._proxy.is_host_alive()
        if not host_list:
            return False

        self._log.debug("Alive hosts '{0}'".format(host_list))
        self._log.debug("Is host '{0}' alive -> '{1}'"
                        .format(host_id, host_id in host_list))
        return host_id in host_list

    def get_stats_from_storage(self):
        """
        Returns data from the shared storage for all hosts of the specified
        service type.
        """
        result = self._proxy.get_stats()
        ret = {}
        for host_id, data in result.items():
            ret[int(host_id)] = data.data.decode()

        return ret

    def start_domain_monitor(self, host_id):
        """
        Starts domain monitoring
        :param host_id: agent's host id
        :return: "ok" in case of success
        """
        self._proxy.start_domain_monitor(host_id)

    def stop_domain_monitor(self):
        """
        Stops domain monitoring
        :return: "ok" in case of success
        """
        self._proxy.stop_domain_monitor()

    def lock_host_id(self, host_id):
        """
        Starts domain monitoring
        :param host_id: agent's host id
        :return: "ok" in case of success
        """
        self._proxy.lock_host_id(host_id)

    def release_host_id(self):
        """
        Stops domain monitoring
        :return: "ok" in case of success
        """
        self._proxy.release_host_id()

    def reset_lockspace(self):
        """
        Clears lockspace data
        :return: "ok" in case of success
        """
        self._proxy.reset_lockspace()
