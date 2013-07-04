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


class IntermittentFilter(logging.Filter):
    """
    Makes sure a messages in each message class are only logged every
    lf_interval seconds or when the message text changes.
    """
    def filter(self, record):
        if (not hasattr(record, 'lf_class')
                or not hasattr(record, 'lf_interval')):
            return True

        log_class = record.lf_class
        if log_class not in self._classes:
            self._classes[record.lf_class] = {'time': None, 'message': None}

        now = time.time()
        if (self._classes[record.lf_class]['time'] is None
                or (self._classes[record.lf_class]['time']
                    + record.lf_interval < now)
                or (self._classes[record.lf_class]['message']
                    != record.get_message())):
            self._classes[record.lf_class]['time'] = now
            self._classes[record.lf_class]['message'] = record.get_message()
            return True
        else:
            return False
