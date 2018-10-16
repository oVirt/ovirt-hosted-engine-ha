#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2018 Red Hat, Inc.
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

import gc
import gzip
import logging
import os
import threading
import time
import pickle

# Import tracemalloc lazily when profile is started
tracemalloc = None

# Defaults
_FRAMES = 25

_lock = threading.Lock()


def start():
    """ Starts application memory profiling """
    global tracemalloc

    logging.debug("Starting memory profiling")

    import tracemalloc

    with _lock:
        if is_running():
            raise RuntimeError('Memory profiler is already running')
        tracemalloc.start(_FRAMES)


def stop():
    """ Stops application memory profiling """
    logging.debug("Stopping memory profiling")
    with _lock:
        if is_running():
            snapshot(_make_snapshot_name)
            tracemalloc.clear_traces()
            tracemalloc.stop()


def is_running():
    return tracemalloc and tracemalloc.is_tracing()


def snapshot(filename=None):
    with _lock:
        if not is_running():
            logging.error('Memory profiler must be running '
                          'to take snapshots')
            return

        name = filename or _make_snapshot_name()
        gc.collect()
        snap = tracemalloc.take_snapshot()
        with gzip.open(name + '.gz', 'wb') as fp:
            # Pickle version 2 can be read by Python 2 and Python 3
            pickle.dump(snap, fp, 2)
            snap = None
        return name


def _make_snapshot_name():
    return os.path.join(
        '/var/run/ovirt-hosted-engine-ha/agent_memory_%s.pickle'
        % _make_timestamp()
    )


def _make_timestamp():
    return time.strftime('%Y%m%d_%H%M%S')
