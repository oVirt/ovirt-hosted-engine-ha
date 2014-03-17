import os
import errno
from abc import ABCMeta, abstractmethod
import subprocess
from ..env import constants
from . import util
import logging
import re
from collections import namedtuple
import zlib
import struct
from io import BytesIO
from operator import itemgetter
import math

logger = logging.getLogger(__name__)


class BlockBackendCorruptedException(Exception):
    """
    Exception raised by BlockBackend when the internal metadata
    structure reports a corrupted data (CRC mismatch).
    """
    pass


class StorageBackend(object):
    """
    The base template for Storage backend classes.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        # the atomic block size of the underlying storage
        self._blocksize = constants.METADATA_BLOCK_BYTES

    @abstractmethod
    def connect(self):
        """Initialize the storage."""
        raise NotImplementedError()

    @abstractmethod
    def disconnect(self):
        """Close the storage."""
        raise NotImplementedError()

    @abstractmethod
    def filename(self, service):
        """
        Return a tuple with the filename to open and bytes to skip
        to get to the metadata structures.
        """
        raise NotImplementedError()

    @property
    def blocksize(self):
        return self._blocksize

    @abstractmethod
    def create(self, service_map, force_new=False):
        """
        Reinitialize the storage backend according to the service_map.
        Key represents service name and value contains the size of the
        required block in Bytes.

        If force_new is True then an update attempt won't be executed
        and fresh storage will be initialized.

        Returns set of services (service_map keys) that were created.
        """
        raise NotImplementedError()


class BackendFailureException(Exception):
    """
    This exception is raised when any backend related error
    happens. The causes include CRC mismatch, impossible operations
    or unexpected errors.
    """
    pass


class FilesystemBackend(StorageBackend):
    """
    Backend for all filesystem based access structures. This
    includes VDSM's LVM block devices as they are accessed using symlinks
    in the same structure that VDSM uses for NFS based storage domains.
    """

    LVSCAN_RE = re.compile("LSize ([0-9]*)B")

    def __init__(self, sd_uuid, dom_type):
        super(FilesystemBackend, self).__init__()
        self._sd_uuid = sd_uuid
        self._dom_type = dom_type
        self._lv_based = False
        self._storage_path = None

    def filename(self, service):
        fname = os.path.join(self._storage_path, service)
        return (fname, 0)

    def get_domain_path(self, sd_uuid, dom_type):
        """
        Return path of storage domain holding the engine vm
        in the form (path, lvm_based_bool)
        """
        parent = constants.SD_MOUNT_PARENT
        if dom_type == 'glusterfs':
            parent = os.path.join(parent, 'glusterSD')

        for dname in os.listdir(parent):
            path = os.path.join(parent, dname, sd_uuid)
            if os.access(path, os.F_OK):
                return path, dname == "blockSD"
        raise BackendFailureException("path to storage domain {0} not found"
                                      " in {1}"
                                      .format(sd_uuid, parent))

    def connect(self):
        base_path, self._lv_based = self.get_domain_path(self._sd_uuid,
                                                         self._dom_type)
        self._storage_path = os.path.join(base_path,
                                          constants.SD_METADATA_DIR)
        if not self._lv_based:
            return

        # create LV symlinks
        uuid = self._sd_uuid
        for lv in os.listdir(os.path.join("/dev", uuid)):
            # skip all LVs that do not have proper name
            if not lv.startswith(constants.SD_METADATA_DIR + "-"):
                continue

            # strip the prefix and use the rest as symlink name
            service = lv.split(constants.SD_METADATA_DIR + "-", 1)[-1]
            service_link = os.path.join(self._storage_path, service)
            try:
                os.unlink(service_link)
                logger.info("Cleaning up stale LV link %s", service_link)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    # If the file is not there it is not a failure,
                    # but if anything else happened, raise it again
                    raise
            os.symlink(os.path.join("/dev", uuid, lv), service_link)

    def disconnect(self):
        pass

    def lvsize(self, vg_uuid, lv_name, popen=subprocess.Popen):
        """
        Call lvs and ask for a size of the Logical Volume.
        If the LV does not exist, return None.
        """
        lvc = popen(stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    args=["lvs", "--rows", "--units", "B", "-o", "lv_size",
                          "/".join([vg_uuid, lv_name])])
        stdout, stderr = lvc.communicate()
        lvc.wait()

        if lvc.returncode == 0:
            m = self.LVSCAN_RE.search(stdout.strip())
            if m is None:
                logger.error("LV for service %s was found in VG %s"
                             " but the size could not be determined"
                             " from lvm's output:\n",
                             lv_name, vg_uuid, stdout)
                return 0

            size = int(m.groups()[0])
            logger.info("LV for service %s was found in VG %s"
                        " and has size %d", lv_name, vg_uuid, size)
            return size
        else:
            logger.error("LV for service %s was NOT found in VG %s",
                         lv_name, vg_uuid)
            return None

    def lvcreate(self, vg_uuid, lv_name, size_bytes, popen=subprocess.Popen):
        """
        Call lvm lvcreate and ask it to create a Logical Volume in the
        Storage Domain's Volume Group. It should be named lv_name
        and be big enough to fit size_bytes into it.
        """
        lvc = popen(stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    args=["lvm", "lvcreate", "-L", str(size_bytes)+"B",
                          "-n", lv_name, vg_uuid])
        stdout, stderr = lvc.communicate()
        lvc.wait()
        if lvc.returncode == 0:
            logger.info("LV for service %s was created in VG %s",
                        lv_name, vg_uuid)
            return True
        else:
            logger.error("LV for service %s was NOT created in VG %s\n%s",
                         lv_name, vg_uuid, stderr)
            return False

    def lvzero(self, vg_uuid, lv_name, size_bytes):
        """
        Zero the first size_bytes of an LV.
        """
        logger.info("Zeroing out LV %s/%s", vg_uuid, lv_name)
        with open(os.path.join("/dev", self._sd_uuid, lv_name), "w") as dev:
            dev.write('\0' * size_bytes)
            dev.flush()

    def _create_block(self, service, size, force_new):
        lvname = "-".join([constants.SD_METADATA_DIR, service])
        cur_size = self.lvsize(self._sd_uuid, lvname)
        if cur_size is None:
            if self.lvcreate(self._sd_uuid, lvname, size):
                self.lvzero(self._sd_uuid, lvname, size)
                return True
        elif size > cur_size:
            err_msg = "LV for service %s already exists but" \
                      " has insufficient size %d" \
                      " (%d needed)" % (service, cur_size, size)
            logger.info(err_msg)
            raise BackendFailureException(err_msg)
        else:
            logger.info("LV for service %s already exists and"
                        " has sufficient size %d",
                        service, cur_size)
            if force_new:
                self.lvzero(self._sd_uuid, lvname, size)
            return force_new

    def _create_file(self, service_path, size, force_new):
        # file based storage
        # truncate the file if force new was requested
        mode = "wb" if force_new else "ab"
        with open(service_path, mode) as f:
            cur_size = f.tell()

            # create a file full of zeros
            if cur_size == 0:
                f.write('\0' * size)
                logger.info("Service file %s was initialized to"
                            " size %d",
                            service_path, size)
                return True
            elif cur_size < size:
                f.write('\0' * (size - cur_size))
                logger.info("Service file %s was enlarged to size"
                            " %d (was %d)",
                            service_path, size, cur_size)
                return True
            else:
                logger.info("Service file %s was already present",
                            service_path)
                return False

    def create(self, service_map, force_new=False):
        base_path, self._lv_based = self.get_domain_path(self._sd_uuid,
                                                         self._dom_type)
        self._storage_path = os.path.join(base_path,
                                          constants.SD_METADATA_DIR)

        util.mkdir_recursive(self._storage_path)

        new_set = set()
        for service, size in service_map.iteritems():
            service_path = os.path.join(self._storage_path, service)
            if self._lv_based:
                isnew = self._create_block(service, size, force_new)
            else:
                isnew = self._create_file(service_path, size, force_new)

            # record all new services
            if isnew:
                new_set.add(service)

        return new_set


class BlockBackend(StorageBackend):
    """
    This uses a pure block device to expose the data. It requires device
    mapper support to explode the single device to couple of virtual files.

    This is supposed to be used for devices that are not managed by VDSM
    or do not use LVM.

    The structure is described using a table that starts at block 0
    of the block device.

    The format of that block is:

    HEs0 - signature sequence
    <the next chained block:64bit> - 0 means this is the last block
    <service name used length: 1 Byte>
    <service name: 63 Bytes>
    <data area start block:64 bit>
    <data area block length:64 bit>
    ... data area records can be repeated if they fit into one block
    ... if there is need for more data area records, one of the chained
    ... blocks can add them to the same service name
    128bit (16B) of 0s as a sentinel
    32bit CRC32

    This information is converted to Device Mapper table and used to create
    the logical device files.
    """

    # Binary format specifications, all in network byte order
    # The name supports only 63 characters
    ValidSignature = "HEs0"
    BlockInfo = namedtuple("BlockInfo", ("signature", "next",
                                         "name", "pieces", "valid"))
    BlockStructHeader = struct.Struct("!4sQ64p")
    BlockStructData = struct.Struct("!QQ")
    BlockCRC = struct.Struct("!L")

    def __init__(self, block_dev_name, dm_prefix):
        super(BlockBackend, self).__init__()
        self._block_dev_name = block_dev_name
        self._dm_prefix = dm_prefix.replace("-", "--")
        self._services = {}

    def parse_meta_block(self, block):
        """
        Parse one info block from the raw byte representation
        to namedtuple BlockInfo.
        """
        sig, next_block, name = self.BlockStructHeader.unpack_from(block, 0)
        pieces = []
        seek = self.BlockStructHeader.size
        while True:
            start, size = self.BlockStructData.unpack_from(block, seek)
            seek += self.BlockStructData.size
            # end of blocks section sentinel
            if start == size and size == 0:
                break
            pieces.append((start, size))
        crc = zlib.crc32(block[:seek]) & 0xffffffff
        # the comma is important, unpack_from returns a single element tuple
        expected_crc, = self.BlockCRC.unpack_from(block, seek)

        return self.BlockInfo._make((sig, next_block, name,
                                     tuple(pieces), crc == expected_crc))

    def get_services(self, block_device_fo):
        """
        Read all the info blocks from a block device and
        assemble the services dictionary mapping
        service name to a list of (data block start, size)
        tuples.

        Returns a tuple (services, last_info_block, first_free_block)
         - services is dictionary service name -> [(start, len), ..]
         - last_info_block is the block that ends the info block chain
           (next is 0)
         - first_free_block is the first unused block on the storage
           device
        """
        last_info_block = 0
        first_free_block = 0
        offset = block_device_fo.tell()
        services = {}
        while True:
            block = block_device_fo.read(self.blocksize)
            parsed = self.parse_meta_block(block)
            if parsed.signature != self.ValidSignature:
                raise BlockBackendCorruptedException(
                    "Signature %s for block ending at %d is not valid!"
                    % (parsed.signature, block_device_fo.tell()))
            if not parsed.valid:
                raise BlockBackendCorruptedException(
                    "CRC for block ending at %d does not match data!"
                    % block_device_fo.tell())
            services.setdefault(parsed.name, [])
            services[parsed.name].extend(parsed.pieces)
            first_free_block = reduce(lambda acc, p: max(acc, p[0]+p[1]),
                                      parsed.pieces,
                                      first_free_block)
            if parsed.next == 0:
                break
            else:
                block_device_fo.seek(offset + parsed.next * self.blocksize, 0)
                last_info_block = parsed.next
        return services, last_info_block, first_free_block

    def dm_name(self, service):
        return "-".join([self._dm_prefix, service.replace("-", "--")])

    def compute_dm_table(self, pieces):
        """
        Take a list of tuples in the form of (start, size) and
        create the string representation of device mapper table
        that can be used in dmsetup.
        """
        table = []
        log_start = 0
        for start, size in pieces:
            table.append("%d %d linear %s %d"
                         % (log_start, size, self._block_dev_name, start))
            log_start += size
        return "\n".join(table)

    def connect(self):
        with open(self._block_dev_name, "rb") as bd:
            self._services, _li, _ff = self.get_services(bd)

        for name, pieces in self._services.iteritems():
            table = self.compute_dm_table(pieces)
            self.dmcreate(name, table)

    def disconnect(self):
        for name in self._services:
            self.dmremove(name)

    def dmcreate(self, name, table, popen=subprocess.Popen):
        """
        Call dmsetup create <name> and pass it the table.
        """
        name = self.dm_name(name)
        dm = popen(stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE,
                   args=["dmsetup", "create", name])
        logger.debug("Table for %s\n%s", name, table)
        stdout, stderr = dm.communicate(table)
        dm.wait()
        logger.debug("dmcreate %s stdout: %s", name, stdout)
        logger.debug("dmcreate %s stderr: %s", name, stderr)
        logger.info("dmcreate %s return code: %d", name, dm.returncode)

    def dmremove(self, name, popen=subprocess.Popen):
        """
        Call dmsetup remove to destroy the device.
        """
        name = self.dm_name(name)
        dm = popen(stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE,
                   args=["dmsetup", "remove", name])
        stdout, stderr = dm.communicate()

        dm.wait()
        logger.debug("dmremove %s stdout: %s", name, stdout)
        logger.debug("dmremove %s stderr: %s", name, stderr)
        logger.info("dmremove %s return code: %d", name, dm.returncode)

    def filename(self, service):
        if service not in self._services:
            return None
        else:
            return os.path.join("/dev/mapper", self.dm_name(service)), 0

    def create_block(self, data_blocks, next_id, service):
        raw_data = BytesIO()
        raw_data.write(self.BlockStructHeader.pack(self.ValidSignature,
                                                   next_id, service))
        for start, length in data_blocks:
            raw_data.write(self.BlockStructData.pack(start, length))
        raw_data.write(self.BlockStructData.pack(0, 0))
        crc = zlib.crc32(raw_data.getvalue()) & 0xffffffff
        raw_data.write(self.BlockCRC.pack(crc))
        return raw_data.getvalue()

    def create_info_blocks(self, service_map, first_free=0):
        def bc(size):
            """
            Return the number of blocks needed to accommodate size
            number of Bytes.
            """
            return int(math.ceil(size / float(self._blocksize)))

        # first len(service_map) blocks will contain
        # the information about services and their data locations
        data_start = len(service_map) + first_free
        info_blocks = []

        # Linearize the list, put smaller services before bigger ones
        service_list = service_map.items()
        service_list.sort(key=itemgetter(1))

        # create list of next ids that starts with 1, goes to the last
        # index (size - 1) and then ends with 0
        next_links = range(first_free + 1, data_start) + [0]
        for next_id, (service, size) in zip(next_links, service_list):
            block_len = bc(size)
            data_blocks = [(data_start, block_len)]
            raw_data = self.create_block(data_blocks, next_id, service)
            info_blocks.append(raw_data)
            data_start += block_len

        return info_blocks

    def _write(self, dev, info_blocks, last_info=None, first_free=0):
        """
        This writes the blocks to the storage device and connects
        them to the existing info chain identified by the ending
        element's block address last_info.
        """

        # Write the new blocks
        for idx, b in enumerate(info_blocks):
            position = (first_free + idx) * self._blocksize
            dev.seek(position)
            dev.write(b)

        # Update the pointer to next block to connect
        # the info block chains together.
        if last_info and info_blocks:
            position = last_info * self._blocksize
            dev.seek(position)

            # read the old block
            raw = dev.read(self._blocksize)
            block = self.parse_meta_block(raw)
            assert block.valid and block.next == 0x0

            block = block._replace(next=first_free)
            raw = self.create_block(block.pieces, first_free, block.name)

            dev.seek(position)
            dev.write(raw)

    def _compute_updates(self, service_map):
        """
        Compute how much more space needs to be allocated
        to accomodate the service_map.

        It returns a new service_map that contains the extra
        space needed for existing services and all space needed
        for newly added services.
        """
        new_service_map = {}
        for k, v in service_map.iteritems():
            if k in self._services:
                size = sum(piece[1] * self._blocksize
                           for piece in self._services[k])
                if size < v:
                    new_service_map[k] = v - size
            else:
                new_service_map[k] = v

        return new_service_map

    def create(self, service_map, force_new=False):
        # if update is requested, read the existing service list
        if not force_new:
            with open(self._block_dev_name, "rb") as bd:
                self._services, last_info, first_free = self.get_services(bd)
        else:
            self._services, last_info, first_free = {}, None, 0

        # find the difference between requested size and existing size
        new_service_map = self._compute_updates(service_map)
        # create the new info blocks
        info_blocks = self.create_info_blocks(new_service_map, first_free)

        # write everything
        with open(self._block_dev_name, "r+b") as dev:
            self._write(dev, info_blocks, last_info, first_free)

        # return the updated services
        return set(new_service_map.keys())
