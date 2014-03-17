import os
import errno
from abc import ABCMeta, abstractmethod
import subprocess
from ..env import constants
from . import util
import logging
import re

logger = logging.getLogger(__name__)


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
