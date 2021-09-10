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
    def __init__(self):
        self._classes = {}

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
        if (self._classes[record.lf_class]['time'] is None or
            (self._classes[record.lf_class]['time'] +
                record.lf_interval < now) or
                (self._classes[record.lf_class]['message'] !=
                    record.getMessage())):
            self._classes[record.lf_class]['time'] = now
            self._classes[record.lf_class]['message'] = record.getMessage()
            return True
        else:
            return False


def lf_args(lf_class, lf_interval):
    return {'lf_class': lf_class,
            'lf_interval': lf_interval}


__intermittent_filter = None


def get_intermittent_filter():
    """
    Use this function when adding the filter to a logger,
    do not instantiate it directly.

    Otherwise one logger instance will have multiple
    instances of the same filter.
    """

    global __intermittent_filter
    if __intermittent_filter is None:
        __intermittent_filter = IntermittentFilter()

    return __intermittent_filter
