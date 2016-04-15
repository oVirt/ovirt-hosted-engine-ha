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
import socket
import time

from otopi import util
from vdsm import vdscli

from exceptions import DetailedError
from ..env import constants


# TODO: move to jsonrpc everywhere and get rid of this
def run_vds_client_cmd(address, use_ssl, command, *args, **kwargs):
    """
    Run the passed in command name from the vdsClient library and either
    throw an exception with the error message or return the results.
    """
    # FIXME pass context to allow for shared or persistent vdsm connection
    log = logging.getLogger('SubmonitorUtil')
    log.debug("Connecting to vdsClient at %s with ssl=%r", address, use_ssl)
    vdsClient = util.loadModule(
        path=constants.VDS_CLIENT_DIR,
        name='vdsClient'
    )
    if vdsClient._glusterEnabled:
        serv = vdsClient.ge.GlusterService()
    else:
        serv = vdsClient.service()
    serv.useSSL = use_ssl

    if hasattr(vdscli, 'cannonizeAddrPort'):
        server, server_port = vdscli.cannonizeAddrPort(
            address
        ).split(':', 1)
        serv.do_connect(server, server_port)
    else:
        host_port = vdscli.cannonizeHostPort(address)
        serv.do_connect(host_port)

    log.debug("Connected, running %s, args %r, kwargs %r",
              command, args, kwargs)

    method = getattr(serv.s, command)
    retry = 0
    response = None
    new_args = list(args)
    # Add keyword args to argument list as a dict for vds api compatibility
    if kwargs:
        new_args.append(kwargs)
    while retry < constants.VDS_CLIENT_MAX_RETRY:
        try:
            response = method(*new_args)
            break
        except socket.error:
            log.debug("Error", exc_info=True)
            retry += 1
            time.sleep(1)

    log.debug("Response: %r", response)
    if retry >= constants.VDS_CLIENT_MAX_RETRY:
        raise Exception("VDSM initialization timeout")
    if response and response['status']['code'] != 0:
        raise DetailedError("Error {0} from {1}: {2}"
                            .format(response['status']['code'],
                                    command,
                                    response['status']['message']),
                            response['status']['message'])
    return response
