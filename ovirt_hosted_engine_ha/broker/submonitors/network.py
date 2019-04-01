#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2013-2019 Red Hat, Inc.
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
import os
import subprocess
import time

from ovirt_hosted_engine_ha.broker import submonitor_base
from ovirt_hosted_engine_ha.lib import log_filter


def register():
    return "network"


class Submonitor(submonitor_base.SubmonitorBase):

    def setup(self, options):
        self._log = logging.getLogger("%s.Network" % __name__)
        self._log.addFilter(log_filter.get_intermittent_filter())
        self._log.debug("options=%s", options)

        self._tests = {
            'ping': self._ping,
            'dns': self._dns,
            'tcp': self._tcp,
            'none': self._none,
        }

        self._addr = options.get('addr')
        self._timeout = str(options.get('timeout', 2))
        self._total = options.get('count', 5)
        self._delay = options.get('delay', 0.5)
        self._network_test = options.get('network_test', 'ping')
        if not self._network_test:
            self._network_test = 'ping'
        if self._network_test not in self._tests:
            raise Exception(
                "{t}: invalid network test".format(
                    t=self._network_test
                )
            )
        self._tcp_t_address = options.get('tcp_t_address', None)
        self._tcp_t_port = options.get('tcp_t_port', None)

        if self._addr is None and self._network_test == 'ping':
            raise Exception("ping requires addr address")

        if (
            (self._tcp_t_address is None or self._tcp_t_port is None) and
            self._network_test == 'tcp'
        ):
            raise Exception("tcp test requires tcp_t_address and tcp_t_port")

        # Set initial result to success instead of None
        self.update_result(1.0)

    def action(self, options):
        count = 0
        test_function = self._tests[self._network_test]
        for i in range(self._total):
            if test_function():
                count += 1

            # wait between tests
            if i < self._total - 1:
                time.sleep(self._delay)

        if count == self._total:
            self._log.info("Successfully verified network status",
                           extra=log_filter.lf_args('status', 60))
        else:
            self._log.warning(
                "Failed to verify network status, (%s out of %s)",
                count, self._total
            )

        self.update_result(float(count) / float(self._total))

    def _ping(self):
        ping_cmd = ['ping', '-c', '1', '-W', self._timeout]
        if ':' in self._addr:
            ping_cmd.append('-6')
        ping_cmd.append(self._addr)
        with open(os.devnull, "w") as devnull:
            p = subprocess.Popen(ping_cmd, stdout=devnull, stderr=devnull)
            return p.wait() == 0

    def _dns(self):
        dns_cmd = [
            'dig',
            '+tries=1',
            '+time={t}'.format(t=self._timeout)
        ]
        with open(os.devnull, "w") as devnull:
            p = subprocess.Popen(dns_cmd, stdout=devnull, stderr=devnull)
            return p.wait() == 0

    def _tcp(self):
        tcp_cmd = [
            'nc',
            '-w',
            self._timeout,
            '-z',
            self._tcp_t_address,
            self._tcp_t_port,
        ]
        with open(os.devnull, "w") as devnull:
            p = subprocess.Popen(tcp_cmd, stdout=devnull, stderr=devnull)
            return p.wait() == 0

    def _none(self):
        return True
