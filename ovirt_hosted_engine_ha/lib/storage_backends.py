import os
import errno
from abc import ABCMeta, abstractmethod
import subprocess
from . import util
import logging
import re
from collections import namedtuple
import math
import time
import uuid

from ..env import constants
from ..lib import image
from ..lib import storage_server

logger = logging.getLogger(__name__)

_StorageBackendTypesTuple = namedtuple(
    'StorageBackendTypes',
    ['FilesystemBackend', 'VdsmBackend']
)

StorageBackendTypes = _StorageBackendTypesTuple(
    FilesystemBackend='FilesystemBackend',
    VdsmBackend='VdsmBackend'
)


class StorageBackend(object):
    """
    The base template for Storage backend classes.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        # the atomic block size of the underlying storage
        self._blocksize = constants.METADATA_BLOCK_BYTES
        self._logger = logger

    @property
    def direct_io(self):
        return True

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
                                      " in {1}".format(sd_uuid, parent))

    def set_external_logger(self, extlogger):
        """
        Let the consumer pass an external logger
        """
        if extlogger:
            self._logger = extlogger

    def _check_symlinks(self, storage_path, volume_path, service_link):
        try:
            os.unlink(service_link)
            self._logger.info("Cleaning up stale LV link '%s'", service_link)
        except OSError as e:
            if e.errno != errno.ENOENT:
                # If the file is not there it is not a failure,
                # but if anything else happened, raise it again
                raise
        util.mkdir_recursive(storage_path)
        try:
            os.symlink(volume_path, service_link)
        except OSError as e:
            raise Exception(
                "'%s' -> '%s' failed: '%s'" % (
                    volume_path,
                    service_link,
                    str(e)
                )
            )


class BackendFailureException(Exception):
    """
    This exception is raised when any backend related error
    happens. The causes include CRC mismatch, impossible operations
    or unexpected errors.
    """
    pass


class VdsmBackend(StorageBackend):
    """
    This storage backend uses regular VDSM volumes to store the service
    metadata files.
    """

    class Device(object):
        __slots__ = ["image_uuid", "volume_uuid", "path"]

        def __init__(self, image_uuid, volume_uuid, path=None):
            self.image_uuid = image_uuid
            self.volume_uuid = volume_uuid
            self.path = path

    # VDSM storage constants
    RAW_FORMAT = 5
    PREALLOCATED_VOL = 1
    DATA_DISK_TYPE = 2
    TASK_WAIT = 1

    # Megabyte constant
    MiB = float(math.pow(2, 20))

    def __init__(self, sp_uuid, sd_uuid, dom_type, **activate_devices):
        """
        :param sp_uuid: Storage Pool UUID (data center)
        :type sp_uuid: str

        :param sd_uuid: Storage domain UUID
        :type sd_uuid: str

        :param dom_type: domain type identifier
        :type dom_type: nfs, glusterSD, ...

        :param activate_devices: Dictionary with service name as a key
                                 and image uuid as value. The services from
                                 this dictionary will be automatically
                                 activated after connect() is called.
        :type activate_devices: dict[str]=VdsmBackend.Device
        """
        super(VdsmBackend, self).__init__()
        self._services = activate_devices
        self._sp_uuid = sp_uuid
        self._sd_uuid = sd_uuid
        self._dom_type = dom_type

    def _get_volume_path(self, connection, spUUID, sdUUID, imgUUID, volUUID):
        retval = namedtuple('retval', ['status_code', 'path', 'message'])
        response = connection.prepareImage(
            storagepoolID=spUUID,
            storagedomainID=sdUUID,
            imageID=imgUUID,
            volumeID=volUUID
        )
        self._logger.debug("prepareImage: '%s'", response)
        path = response.get('path', None)
        self._logger.debug("prepareImage: returned '%s' as path", path)
        return retval(response["status"]["code"], path,
                      response["status"].get("message", ""))

    def create_volume(self, service_name, service_size,
                      volume_uuid, image_uuid):
        """
        :param service_name: a string identifying the service
        :param service_size: size of the requested space in Bytes
        :param volume_uuid: a UUID to identify the volume in VDSM
        :param image_uuid: a UUID to identify the underlying file image

        Returns True if the volume has been created, False otherwise.
        Raise RuntimeError if something goes wrong.
        """
        if self._sp_uuid is None:
            raise RuntimeError(
                "Storage Pool must be defined for creating volumes"
            )

        # Connect to local VDSM
        self._logger.debug("Connecting to VDSM")
        connection = util.connect_vdsm_json_rpc(logger=self._logger)

        # Check of the volume already exists
        response = self._get_volume_path(
            connection,
            sdUUID=self._sd_uuid,
            spUUID=self._sp_uuid,
            imgUUID=image_uuid,
            volUUID=volume_uuid
        )

        if response.status_code == 0:
            self._logger.info("Image for '%s' already exists", service_name)
            path = response['path']
            return False, path

        elif response.status_code not in (201, 254):
            # 201 is returned when the volume doesn't exist
            # 254 is returned when Image path doesn't exist
            raise RuntimeError(response["status"]["message"])

        self._logger.info("Creating Image for '%s' ...", service_name)

        # Create new volume
        create_response = connection.createVolume(
            volumeID=volume_uuid,
            storagepoolID=self._sp_uuid,
            storagedomainID=self._sd_uuid,
            imageID=image_uuid,
            size=str(service_size),  # Need to be str for using bytes.
            volFormat=self.RAW_FORMAT,
            preallocate=self.PREALLOCATED_VOL,
            diskType=self.DATA_DISK_TYPE,
            desc=service_name,
            srcImgUUID=constants.BLANK_UUID,
            srcVolUUID=constants.BLANK_UUID,
        )

        self._logger.debug("createVolume: '%s'", create_response)
        if create_response["status"]["code"] != 0:
            raise RuntimeError(create_response["status"]["message"])

        # Wait for the createVolume task to finish
        task = create_response["status"]["message"]
        while True:
            task_status = connection.getTaskStatus(taskID=task)
            self._logger.debug("getTaskStatus: '%s'" % str(task_status))
            if task_status["status"]["code"] != 0:
                raise RuntimeError(task_status["status"]["message"])
            elif task_status["taskState"] == "finished":
                if task_status["taskResult"] != "success":
                    raise RuntimeError(task_status["taskResult"])
                break
            else:
                time.sleep(self.TASK_WAIT)

        response = connection.clearTask(taskID=task)
        self._logger.debug("clearTask: '%s'", response)
        self._logger.info("Image for '%s' created successfully", service_name)

        response = self._get_volume_path(
            connection, sdUUID=self._sd_uuid,
            spUUID='00000000-0000-0000-0000-000000000000',
            imgUUID=image_uuid,
            volUUID=volume_uuid
        )
        if response.status_code != 0:
            raise RuntimeError(response.message)

        # Clear the volume (VDSM does not do that automatically for iSCSI)
        # use 10KiB blocks to do the writing
        BLOCK_SIZE = 10240
        # Find out file size by opening it and seeking till the end.
        # Works on regular files, block devices, symlinks to them etc.
        fd = os.open(response.path, os.O_RDONLY)
        try:
            remaining_size = os.lseek(fd, 0, os.SEEK_END)
        finally:
            os.close(fd)
        with open(response.path, 'r+') as of:
            while remaining_size > 0:
                of.write('\0' * (BLOCK_SIZE if remaining_size > BLOCK_SIZE
                                 else remaining_size))
                remaining_size -= BLOCK_SIZE
            of.flush()

        return True, response.path

    def create(self, service_map, force_new=False):
        base_path, self._lv_based = self.get_domain_path(self._sd_uuid,
                                                         self._dom_type)
        self._storage_path = os.path.join(base_path,
                                          constants.SD_METADATA_DIR)

        util.mkdir_recursive(self._storage_path)

        new_set = set()
        for service, size in service_map.iteritems():
            # Generate new random UUIDs
            new_image_uuid = str(uuid.uuid4())
            new_volume_uuid = str(uuid.uuid4())

            isnew, path = self.create_volume(service_name=service,
                                             image_uuid=new_image_uuid,
                                             volume_uuid=new_volume_uuid,
                                             service_size=size)

            if isnew:
                new_set.add(service)
                self._services[service] = self.Device(
                    image_uuid=new_image_uuid,
                    volume_uuid=new_volume_uuid,
                    path=path
                )

        return new_set

    def connect(self, initialize=True):
        """Initialize the storage."""
        # Connect to local VDSM
        self._logger.debug("Connecting to VDSM")
        connection = util.connect_vdsm_json_rpc(logger=self._logger)

        if initialize:
            self._logger.info("Connecting the storage")
            sserver = storage_server.StorageServer()
            img = image.Image(
                self._dom_type,
                self._sd_uuid
            )

            sserver.connect_storage_server()

            self._logger.info("Preparing images")
            img.prepare_images()

        base_path, self._lv_based = self.get_domain_path(self._sd_uuid,
                                                         self._dom_type)
        self._storage_path = os.path.join(base_path,
                                          constants.SD_METADATA_DIR)

        for service, volume in self._services.iteritems():
            # Activate volumes and set the volume.path to proper path
            response = self._get_volume_path(
                connection,
                sdUUID=self._sd_uuid,
                # we're not connected to any storage pool, so we need to use
                # blank pool uuid suggested by fsimonce rhbz#1130038
                spUUID='00000000-0000-0000-0000-000000000000',
                imgUUID=volume.image_uuid,
                volUUID=volume.volume_uuid
            )
            if response.status_code != 0:
                raise RuntimeError(response.message)

            volume.path = response.path

            # Create symlinks for compatibility reasons
            service_link = os.path.join(self._storage_path, service)
            self._check_symlinks(self._storage_path, volume.path, service_link)

    def get_device(self, service):
        """
        This method gives access to the underlying Device objects so it is
        possible to retrieve the volume and image UUIDs.
        """
        return self._services[service]

    def disconnect(self):
        """Close the storage."""
        pass

    def filename(self, service):
        """
        Return a tuple with the filename to open and bytes to skip
        to get to the metadata structures.
        """
        return self._services[service].path, 0


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

    @StorageBackend.direct_io.getter
    def direct_io(self):
        return True

    def filename(self, service):
        fname = os.path.join(self._storage_path, service)
        return (fname, 0)

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
            self._check_symlinks(self._storage_path,
                                 os.path.join("/dev", uuid, lv), service_link)

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
                self._logger.error(
                    "LV for service %s was found in VG %s"
                    " but the size could not be determined"
                    " from lvm's output:\n",
                    lv_name,
                    vg_uuid,
                    stdout
                )
                return 0

            size = int(m.groups()[0])
            self._logger.info(
                "LV for service %s was found in VG %s"
                " and has size %d",
                lv_name,
                vg_uuid,
                size
            )
            return size
        else:
            self._logger.error(
                "LV for service %s was NOT found in VG %s",
                lv_name,
                vg_uuid
            )
            return None

    def lvcreate(self, vg_uuid, lv_name, size_bytes, popen=subprocess.Popen):
        """
        Call lvm lvcreate and ask it to create a Logical Volume in the
        Storage Domain's Volume Group. It should be named lv_name
        and be big enough to fit size_bytes into it.
        """
        lvc = popen(stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    args=["lvm", "lvcreate", "-L", str(size_bytes) + "B",
                          "-n", lv_name, vg_uuid])
        stdout, stderr = lvc.communicate()
        lvc.wait()
        if lvc.returncode == 0:
            self._logger.info(
                "LV for service %s was created in VG %s",
                lv_name,
                vg_uuid
            )
            return True
        else:
            self._logger.error(
                "LV for service %s was NOT created in VG %s\n%s",
                lv_name,
                vg_uuid,
                stderr
            )
            return False

    def lvzero(self, vg_uuid, lv_name, size_bytes):
        """
        Zero the first size_bytes of an LV.
        """
        self._logger.info("Zeroing out LV %s/%s", vg_uuid, lv_name)
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
            self._logger.info(err_msg)
            raise BackendFailureException(err_msg)
        else:
            self._logger.info(
                "LV for service %s already exists and"
                " has sufficient size %d",
                service,
                cur_size
            )
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
                self._logger.info(
                    "Service file %s was initialized to"
                    " size %d",
                    service_path,
                    size
                )
                return True
            elif cur_size < size:
                f.write('\0' * (size - cur_size))
                self._logger.info(
                    "Service file %s was enlarged to size"
                    " %d (was %d)",
                    service_path,
                    size,
                    cur_size
                )
                return True
            else:
                self._logger.info(
                    "Service file %s was already present",
                    service_path
                )
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
