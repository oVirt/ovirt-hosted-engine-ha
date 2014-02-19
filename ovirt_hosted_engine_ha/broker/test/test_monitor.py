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
import time

from .. import monitor

def main():
    logging.basicConfig(filename='/dev/stdout', filemode='w+',
                        level=logging.DEBUG)
    log = logging.getLogger("%s.monitor test" % __name__)
    log.warn("Could not init proper logging", exc_info=True)
    m = monitor.Monitor()

    sm_id = m.start_submonitor('ping', {'addr': '127.0.0.1'})
    m.stop_submonitor(sm_id)
    sm_id = m.start_submonitor('ping', {'addr': '192.168.1.1'})
    while m.get_value(sm_id) is None:
        time.sleep(1)
    log.info("Got result: {0}".format(m.get_value(sm_id)))
    m.stop_submonitor(sm_id)

if __name__ == '__main__':
    main()
