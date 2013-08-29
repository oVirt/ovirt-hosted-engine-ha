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

from ..env import constants
from exceptions import MetadataError


def parse_metadata_to_dict(host_str, data):
    try:
        host_id = int(host_str)
    except ValueError:
        raise MetadataError("Malformed metadata:"
                            " host id '{0}' not an integer"
                            .format(host_id))

    if len(data) < 512:
        raise MetadataError("Malformed metadata for host {0}:"
                            " received {1} of {2} expected bytes"
                            .format(host_id, len(data), 512))

    tokens = data[:512].rstrip('\0').split('|')
    if len(tokens) < 7:
        raise MetadataError("Malformed metadata for host {0}:"
                            " received {1} of {2} expected tokens"
                            .format(host_id, len(tokens), 7))

    try:
        md_parse_vers = int(tokens[0])
    except ValueError:
        raise MetadataError("Malformed metadata for host {0}:"
                            " non-parsable metadata version {1}"
                            .format(host_id, tokens[0]))

    if md_parse_vers > constants.METADATA_FEATURE_VERSION:
        raise MetadataError("Metadata version {0} for host {1}"
                            " too new for this agent ({2})"
                            .format(md_parse_vers, host_id,
                                    constants.METADATA_FEATURE_VERSION))

    ret = {
        'host-id': host_id,
        'host-ts': int(tokens[2]),
        'score': int(tokens[4]),
        'engine-status': str(tokens[5]),
        'hostname': str(tokens[6]),
    }

    # Add human-readable summary from bytes 512+
    extra = data[512:].rstrip('\0')
    if len(extra):
        ret['extra'] = extra

    return ret
