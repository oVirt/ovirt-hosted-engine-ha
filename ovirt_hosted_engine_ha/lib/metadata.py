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

import re
import time

from ..env import constants
from ..lib import util
from exceptions import FatalMetadataError
from exceptions import MetadataError


def to_bool_rep(value):
    """
    Transformation function for global metadata.  Serves to verify input as
    a parsable boolean representation and normalize it to 0 or 1.
    """
    return 1 if util.to_bool(value) else 0


# Dict of global metadata flags (keys) and optional verification/transformation
# functions (values) through which global metadata input values are passed.  If
# no such function is needed for a given key, the value should be None.
global_flags = {
    'maintenance': to_bool_rep,
}


def parse_global_metadata_to_dict(log, data):
    """
    Parses a string representing global metadata to a dictionary.  The input
    should be a newline-delimited set of key=value entries, padded with \0
    to HOST_SEGMENT_BYTES in size (4KiB) (i.e. the global metadata block from
    shared storage).  The output is a dict of key=value entries.
    """
    if len(data) < constants.HOST_SEGMENT_BYTES:
        raise MetadataError("Malformed global metadata:"
                            " received {0} of {1} expected bytes"
                            .format(len(data), constants.HOST_SEGMENT_BYTES))

    ret = {}
    tokens = data[:constants.HOST_SEGMENT_BYTES].rstrip('\0').split('\n')
    for token in tokens:
        k, v = token.split('=')
        if k == 'maintenance':
            ret['maintenance'] = util.to_bool(v)
        else:
            log.error("Invalid global metadata key: {0}".format(token))

    return ret


def create_global_metadata_from_dict(md_dict):
    """
    Creates a string representation of a global metadata dict which is suitable
    for use as the global metadata block in shared storage, as well as parsable
    by the parse_global_metadata_to_dict() function.  Input is a dict of
    key=value entries, and output is a newline-delimited string of "key=value"
    lines which is padded to a total size of HOST_SEGMENT_BYTES (4KiB).
    """
    block = ''
    for k, v in md_dict.iteritems():
        block += '{k}={v}'.format(k=k, v=v)
    block = block.ljust(constants.HOST_SEGMENT_BYTES, '\0')
    return block


def parse_metadata_to_dict(host_str, data):
    """
    Parses a string representing host metadata, returning a dictionary.
    The host_str input is a numeric host_id, and the data is a host stats
    block of global metadata which may be from the shared storage.

    The block is formatted as:
     - First 512 bytes: a pipe-delimited string of values:
         metadata parse version - earliest metadata version this block is
            backwards-compatible with
         metadata feature version - latest metadata version this block is
            compatible with
         timestamp integer - timestamp when this data was created,
            according to the host the data describes
         host id - numeric host identifier
         score - numeric score of this host (see comment for method
            HostedEngine:_generate_lock_blocks() for details)
         engine_status - string status of engine on host, one of
            "vm-down", "vm-up bad-health-status", "vm-up good-health-status",
            (see HostedEngine:engine_status_score_lookup for full/updated
            list of possibilities)
         name - hostname of described host
         maintenance - 0 or 1 representing host is operational or in
            local maintenance
     - Next 3584 bytes (for a total of 4096): human-readable description of
       data to aid in debugging, including factors considered in the host score

    The output is a dict with keys corresponding to the above:
      host-id, host-ts, score, engine-status, hostname
    It also contains a key, "extra", containing the human-readable portaion of
    the metadata block.
    """
    try:
        host_id = int(host_str)
    except ValueError:
        raise MetadataError("Malformed metadata:"
                            " host id '{0}' not an integer"
                            .format(host_str))

    if len(data) < 512:
        raise MetadataError("Malformed metadata for host {0}:"
                            " received {1} of {2} expected bytes"
                            .format(host_id, len(data), 512))

    pdata = data[:512].rstrip('\0')

    try:
        # The md version is the first string of numbers in the metadata,
        # which allows for future changes to the delimiter.
        m = re.match(r'\d+', pdata)
        md_parse_vers = int(m.group())
    except (AttributeError, ValueError):
        raise MetadataError("Malformed metadata for host {0}:"
                            " non-parsable metadata version {1}"
                            .format(host_id, pdata))

    if md_parse_vers > constants.METADATA_FEATURE_VERSION:
        # Another agent in the cluster is writing newer metadata.  Raise a
        # fatal error so the caller knows to stop processing.
        raise FatalMetadataError("Metadata version {0} from host {1}"
                                 " too new for this agent"
                                 " (highest compatible version: {2})"
                                 .format(md_parse_vers, host_id,
                                         constants.METADATA_FEATURE_VERSION))

    tokens = pdata.split('|')

    if len(tokens) < 7:
        raise MetadataError("Malformed metadata for host {0}:"
                            " received {1} of {2} expected tokens"
                            .format(host_id, len(tokens), 7))

    try:
        md_feature_vers = int(tokens[1])
    except ValueError:
        raise MetadataError("Malformed metadata for host {0}:"
                            " non-parsable metadata version {1}"
                            .format(host_id, tokens[1]))

    if md_feature_vers < constants.METADATA_PARSE_VERSION:
        # Our metadata is incompatible; we'll ignore the old agent's metadata
        # and it will ignore ours.
        raise MetadataError("Metadata version {0} from host {1}"
                            " too old for this agent"
                            " (lowest compatible version: {2})"
                            .format(md_feature_vers, host_id,
                                    constants.METADATA_PARSE_VERSION))

    ret = {
        'host-id': host_id,
        'host-ts': int(tokens[2]),
        'score': int(tokens[4]),
        'engine-status': str(tokens[5]),
        'hostname': str(tokens[6]),
    }

    # support maintenance flag if present, but ignore if it isn't
    if len(tokens) >= 8:
        ret['maintenance'] = int(tokens[7]) > 0

    # check the timestamp to get the hosts liveness
    last_valid_time = time.time() - constants.HOST_ALIVE_TIMEOUT_SECS
    ret['live-data'] = ret['host-ts'] >= last_valid_time

    # Add human-readable summary from bytes 512+
    extra = data[512:].rstrip('\0')
    if len(extra):
        ret['extra'] = extra

    return ret
