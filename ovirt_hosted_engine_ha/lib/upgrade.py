#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2015-2016 Red Hat, Inc.
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


from ovirt_hosted_engine_ha.env import config
from ovirt_hosted_engine_ha.env import constants
from ovirt_hosted_engine_ha.lib import heconflib
from ovirt_hosted_engine_ha.lib import image

import os
import tempfile
import logging
from . import log_filter
from . import util
import uuid
import selinux
import subprocess
import time


logger = logging.getLogger(__name__)


class Upgrade(object):
    """
    Handle the upgrade from previous release process.
    """

    def __init__(self):
        self._log = logging.getLogger("%s.StorageServer" % __name__)
        self._log.addFilter(log_filter.IntermittentFilter())
        self._config = config.Config(logger=self._log)
        self._cli = util.connect_vdsm_json_rpc(
            logger=self._log,
            timeout=constants.VDSCLI_SSL_TIMEOUT
        )

        self._type = self._config.get(config.ENGINE, config.DOMAIN_TYPE)
        self._spUUID = self._config.get(config.ENGINE, config.SP_UUID)
        self._sdUUID = self._config.get(config.ENGINE, config.SD_UUID)
        self._storage = self._config.get(config.ENGINE, config.STORAGE)
        self._HEVMID = self._config.get(config.ENGINE, config.HEVMID)
        self._host_id = int(self._config.get(config.ENGINE, config.HOST_ID))
        self._fake_sd_size = '2G'
        self._vfstype = 'ext3'

        self._vm_img_uuid = self._config.get(
            config.ENGINE,
            config.VM_DISK_IMG_ID
        )
        vm_vol_uuid = None
        try:
            vm_vol_uuid = self._config.get(
                config.ENGINE,
                config.VM_DISK_VOL_ID
            )
        except (KeyError, ValueError):
            vm_vol_uuid_list = self._cli.getVolumesList(
                imageID=self._vm_img_uuid,
                storagepoolID=self._spUUID,
                storagedomainID=self._sdUUID,
            )
            if (
                vm_vol_uuid_list['status']['code'] == 0 and
                'items' in vm_vol_uuid_list
            ):
                vm_vol_uuid = vm_vol_uuid_list['items'][0]
        self._vm_vol_uuid = vm_vol_uuid

        self._conf_imgUUID = None
        self._conf_volUUID = None
        self._metadata_imgUUID = self._config.get(
            config.ENGINE,
            config.METADATA_IMAGE_UUID,
        )
        self._metadata_volUUID = self._config.get(
            config.ENGINE,
            config.METADATA_VOLUME_UUID,
        )
        self._lockspace_imgUUID = self._config.get(
            config.ENGINE,
            config.LOCKSPACE_IMAGE_UUID,
        )
        self._lockspace_volUUID = self._config.get(
            config.ENGINE,
            config.LOCKSPACE_VOLUME_UUID,
        )
        self._fake_SD_path = None
        self._fake_file = None
        self._fake_mastersd_uuid = str(uuid.uuid4())
        self._selinux_enabled = selinux.is_selinux_enabled()
        self._fake_master_connection_uuid = str(uuid.uuid4())

    def _execute(self, args, raiseOnError=True):
        self._log.debug("executing: '{cmd}'".format(cmd=' '.join(args)))
        command = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = command.communicate()
        rc = command.wait()
        self._log.debug('rc: ' + str(rc))
        self._log.debug('stdout: ' + str(stdout))
        self._log.debug('stderr: ' + str(stderr))
        if raiseOnError and rc != 0:
            raise RuntimeError(
                'Error executing: '
                '{rc} - stdout:{stdout} - stderr:{stderr}'.format(
                    rc=rc,
                    stdout=stdout,
                    stderr=stderr,
                )
            )
        return rc, stdout, stderr

    def is_conf_file_uptodate(self):
        uptodate = False
        try:
            volume = self._config.get(config.ENGINE, config.CONF_VOLUME_UUID)
            self._log.debug('Conf volume: %s ' % volume)
            _image = self._config.get(config.ENGINE, config.CONF_IMAGE_UUID)
            self._log.debug('Conf image: %s ' % _image)
            spuuid = self._config.get(config.ENGINE, config.SP_UUID)
            if spuuid == constants.BLANK_UUID:
                uptodate = True
            else:
                self._log.debug("Storage domain UUID is not blank")
        except (KeyError, ValueError):
            uptodate = False
        return uptodate

    def _is_conf_volume_there(self):
        """
        It tries to detect the configuration volume since another host could
        create it before us. The detection is based on the configuration volume
        description which is hardcoded.
        Engine, lockspace and metadata images are excluded from the scan since
        we already know their content.
        """
        self._log.info('Looking for conf volume')
        isconfvolume = False
        self._conf_imgUUID = None
        self._conf_volUUID = None

        img = image.Image(self._type, self._sdUUID)
        imageslist = img.get_images_list(self._cli)

        self._log.debug('found images: ' + str(imageslist))
        # excluding engine, metadata and lockspace images
        unknowimages = set(imageslist) - set([
            self._vm_img_uuid,
            self._metadata_imgUUID,
            self._lockspace_imgUUID
        ])
        self._log.debug('candidate images: ' + str(unknowimages))
        for img_uuid in unknowimages:
            volumeslist = self._cli.getVolumesList(
                imageID=img_uuid,
                storagepoolID=self._spUUID,
                storagedomainID=self._sdUUID,
            )
            self._log.debug(volumeslist)
            if volumeslist['status']['code'] != 0:
                # avoid raising here since after a reboot we
                # didn't called prepareImage on all the possible images
                self._log.debug(
                    'Error fetching volumes for {image}: {message}'.format(
                        image=image,
                        message=volumeslist['status']['message'],
                    )
                )
            else:
                vl = volumeslist['items'] if 'items' in volumeslist else []
                for vol_uuid in vl:
                    volumeinfo = self._cli.getVolumeInfo(
                        volumeID=vol_uuid,
                        imageID=img_uuid,
                        storagepoolID=self._spUUID,
                        storagedomainID=self._sdUUID,
                    )
                    self._log.debug(volumeinfo)
                    if volumeinfo['status']['code'] != 0:
                        raise RuntimeError(volumeinfo['status']['message'])
                    description = volumeinfo['description']
                    if description == constants.CONF_IMAGE_DESC:
                        self._conf_imgUUID = img_uuid
                        self._conf_volUUID = vol_uuid
                        isconfvolume = True
                        self._log.info(
                            'Found conf volume: '
                            'imgUUID:{img}, volUUID:{vol}'.format(
                                img=self._conf_imgUUID,
                                vol=self._conf_volUUID,
                            )
                        )
        if self._conf_imgUUID is None or self._conf_volUUID is None:
            self._log.error('Unable to find HE conf volume')
        return isconfvolume

    def _update_conf_file_35_36(self, orig):
        self._log.debug('orig content:\n%s\n' % orig)
        origlines = orig.split('\n')
        modifiedlines = []
        for line in origlines:
            if not (
                line.startswith(config.CONF_VOLUME_UUID) or
                line.startswith(config.CONF_IMAGE_UUID) or
                line.startswith(config.CONF_FILE) or
                line.startswith(config.VM_DISK_VOL_ID) or
                line.startswith(config.SP_UUID)
            ):
                modifiedlines.append(line)

        for key, value in [
            [config.CONF_VOLUME_UUID, self._conf_volUUID],
            [config.CONF_IMAGE_UUID, self._conf_imgUUID],
            [config.CONF_FILE, constants.LOCAL_VM_CONF_PATH],
            [config.VM_DISK_VOL_ID, self._vm_vol_uuid],
            [config.SP_UUID, constants.BLANK_UUID],
        ]:
            modifiedlines.append(
                "{key}={value}".format(
                    key=key,
                    value=value,
                )
            )
        modified = '\n'.join(modifiedlines)
        self._log.debug('modified content:\n%s\n' % modified)
        return modified

    def _fix_path_in_conf_file(self, orig):
        self._log.debug('orig content:\n%s\n' % orig)
        origlines = orig.splitlines()
        modifiedlines = []
        for line in origlines:
            if line.startswith(config.STORAGE):
                fixed = "{key}={value}".format(
                    key=config.STORAGE,
                    value=os.path.normpath(self._storage),
                )
                modifiedlines.append(fixed)
            else:
                modifiedlines.append(line)
        modified = '\n'.join(modifiedlines)
        self._log.debug('modified content:\n%s\n' % modified)
        return modified

    def _create_shared_conf_volume(self, imguuid, voluuid, sizegb):
        self._conf_imgUUID = imguuid
        self._conf_volUUID = voluuid
        self._log.info(
            'Creating hosted-engine configuration volume '
            'on the shared storage domain'
        )

        diskType = 2
        heconflib.create_and_prepare_image(
            self._log,
            self._cli,
            constants.VolumeFormat.RAW_FORMAT,
            constants.VolumeTypes.PREALLOCATED_VOL,
            self._sdUUID,
            self._spUUID,
            self._conf_imgUUID,
            self._conf_volUUID,
            diskType,
            sizegb,
            constants.CONF_IMAGE_DESC,
        )
        #  TODO: add this volume to the engine to prevent misuse

    def _isStoragePoolConnected(self):
        status = self._cli.getConnectedStoragePoolsList()
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Unable to fetch the list of connected SP: {message}'.format(
                    message=status['status']['message'],
                )
            )
        splist = status['items'] if 'items' in status else []
        return self._spUUID in splist

    def _safeConnectStoragePool(self, master, dom_dict):
        try:
            self._connectStoragePool(master, dom_dict)
        except RuntimeError, err:
            if err.args > 1 and err.args[1] == 324:
                # 324 means that the master storage domain is not what we
                # expect, since the hosted-engine bootstrap storage pools
                # should contain only the hosted-engine storage domain (plus
                # the fake storage domain just in the middle of the upgrade)
                # we can try to reconstruct to revert the initial 3.5 status
                self._log.error(
                    'The storagePool looks dirty probably as the result of a '
                    'previous upgrade attempt, trying to revert to 3.5 '
                    'status before the upgrade'
                )
                self._reconstructMaster(master, dom_dict)
                self._connectStoragePool(master, dom_dict)
            else:
                raise

    def _connectStoragePool(self, master, dom_dict):
        self._log.info(
            "Connecting storage pool - "
            "master '{master}' - dom_dict '{dom_dict}'".format(
                master=master,
                dom_dict=dom_dict,
            )
        )
        scsi_key = self._spUUID
        master_ver = 1

        status = self._cli.connectStoragePool(
            storagepoolID=self._spUUID,
            hostID=self._host_id,
            scsiKey=scsi_key,
            masterSdUUID=master,
            masterVersion=master_ver,
            domainDict=dom_dict,
        )
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Unable to connect SP: {message}'.format(
                    message=status['status']['message'],
                ),
                status['status']['code'],
            )

    def _disconnectStoragePool(self):
        self._log.info('Disconnecting storage pool')
        scsi_key = self._spUUID
        status = self._cli.disconnectStoragePool(
            storagepoolID=self._spUUID,
            hostID=self._host_id,
            scsiKey=scsi_key,
        )
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Unable to disconnect SP: {message}'.format(
                    message=status['status']['message'],
                )
            )

    def _get_conffile_content(self, type, update_func=None):
        self._log.info('Reading conf file: %s' % str(type))
        content = None
        path = None

        if type == constants.HEConfFiles.HECONFD_ANSWERFILE:
            path = constants.ANSWER_FILE_35
        elif type == constants.HEConfFiles.HECONFD_HECONF:
            path = constants.ENGINE_SETUP_CONF_FILE
        elif type == constants.HEConfFiles.HECONFD_BROKER_CONF:
            path = constants.NOTIFY_CONF_FILE
        elif type == constants.HEConfFiles.HECONFD_VM_CONF:
            path = constants.VM_CONF_FILE_35

        if path:
            try:
                with open(path, 'r') as f:
                    content = f.read()
            except (OSError, IOError) as ex:
                err = (
                    "Failed to read configuration file '{path}': {ex}"
                ).format(
                    path=path,
                    ex=str(ex),
                )
                self._log.error(err)
                raise RuntimeError(err)
        self._log.debug('--content--\n%s\n' % str(content))
        if not content:
            err = "'{path}' is empty".format(path=path)
            self._log.error(err)
            raise RuntimeError(err)
        if update_func:
            content = update_func(content)
        return content

    def _create_conf_tar(self, answer_c, heconf_c, brokerconf_c, vmconf_c):
        self._log.info(
            'Saving hosted-engine configuration '
            'on the shared storage domain'
        )
        dest = heconflib.get_volume_path(
            self._type,
            self._sdUUID,
            self._conf_imgUUID,
            self._conf_volUUID
        )
        heconflib.create_heconfimage(
            self._log,
            answer_c,
            heconf_c,
            brokerconf_c,
            vmconf_c,
            dest
        )

    def _attach_loopback_device(self):
        if not self._fake_file:
            self._fake_file = tempfile.mkstemp(
                dir=constants.OVIRT_HOSTED_ENGINE_LB_DIR
            )[1]
        os.chown(
            self._fake_file,
            constants.VDSMEnv.VDSM_UID,
            constants.VDSMEnv.KVM_GID,
        )
        self._execute(
            args=(
                'truncate',
                '--size=%s' % self._fake_sd_size,
                self._fake_file
            ),
            raiseOnError=True
        )
        rc, stdout, stderr = self._execute(
            args=(
                'sudo',
                '-n',
                'losetup',
                '--find',
                '--show',
                '--sizelimit=%s' % self._fake_sd_size,
                self._fake_file,
            ),
            raiseOnError=True
        )
        if rc == 0:
            for line in stdout.split('\n'):
                if len(line) > 1:
                    self._fake_SD_path = line
            if not self._fake_SD_path:
                raise RuntimeError(
                    'Unable to find an available loopback device path '
                )
            if self._fake_SD_path[:9] != '/dev/loop':
                raise RuntimeError(
                    "Invalid loopback device path name: '{path}'".format(
                        path=self._fake_SD_path,
                    )
                )
            self._log.debug(
                'Found a available loopback device on %s' % self._fake_SD_path
            )

        self._execute(
            args=(
                'sudo',
                '-n',
                'mkfs',
                '-t',
                self._vfstype,
                self._fake_SD_path,
            ),
            raiseOnError=True
        )
        mntpoint = tempfile.mkdtemp(
            dir=constants.OVIRT_HOSTED_ENGINE_LB_DIR
        )
        self._execute(
            args=(
                'sudo',
                '-n',
                'mount',
                self._fake_SD_path,
                mntpoint
            ),
            raiseOnError=True
        )
        self._execute(
            args=(
                'sudo',
                '-n',
                'chown',
                '-R',
                'vdsm',
                mntpoint,
            ),
            raiseOnError=True
        )
        if self._selinux_enabled:
            con = "system_u:object_r:virt_var_lib_t:s0"
            selinux.chcon(path=mntpoint, context=con, recursive=True)
        self._execute(
            args=(
                'sudo',
                '-n',
                'umount',
                mntpoint,
            ),
            raiseOnError=True
        )
        os.rmdir(mntpoint)

    def _remove_loopback_device(self):
        if self._fake_SD_path:
            self._execute(
                args=(
                    'sudo',
                    '-n',
                    'losetup',
                    '--detach',
                    self._fake_SD_path,
                ),
                raiseOnError=True
            )
            self._fake_SD_path = None
        if self._fake_file:
            os.unlink(self._fake_file)
            self._fake_file = None

    def _createFakeStorageDomain(self):
        self._log.info('createFakeStorageDomain')
        storageType = constants.VDSMConstants.POSIXFS_DOMAIN
        sdUUID = self._fake_mastersd_uuid
        domainName = 'FakeHostedEngineStorageDomain'
        typeSpecificArgs = self._fake_file
        domainType = constants.VDSMConstants.DATA_DOMAIN
        version = 3
        status = self._cli.createStorageDomain(
            storagedomainID=sdUUID,
            domainType=storageType,
            name=domainName,
            typeArgs=typeSpecificArgs,
            domainClass=domainType,
            version=version
        )
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(status['status']['message'])
        self._log.debug(self._cli.repoStats())
        self._log.debug(
            self._cli.getStorageDomainStats(sdUUID)
        )

    def _connectFakeStorageDomainServer(self):
        self._log.info('connectFakeStorageDomainServer')
        fakeSDconList = [{
            'connection': self._fake_file,
            'spec': self._fake_file,
            'vfsType': self._vfstype,
            'id': self._fake_master_connection_uuid,
        }]
        status = self._cli.connectStorageServer(
            storagepoolID=self._spUUID,
            domainType=constants.VDSMConstants.POSIXFS_DOMAIN,
            connectionParams=fakeSDconList,
        )
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Unable to connect FakeStorageDomainServer: ' +
                str(status['status']['message'])
            )

    def _disconnectFakeStorageDomainServer(self):
        self._log.info('_disconnectFakeStorageDomainServer')
        fakeSDconList = [{
            'connection': self._fake_file,
            'spec': self._fake_file,
            'vfsType': self._vfstype,
            'id': self._fake_master_connection_uuid,
        }]
        status = self._cli.disconnectStorageServer(
            storagepoolID=self._spUUID,
            domainType=constants.VDSMConstants.POSIXFS_DOMAIN,
            connectionParams=fakeSDconList,
        )
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Unable to disconnect FakeStorageDomainServer: ' +
                str(status['status']['message'])
            )

    def _attachFakeStorageDomain(self):
        self._log.info('_attachFakeStorageDomain')
        status = self._cli.attachStorageDomain(
            storagedomainID=self._fake_mastersd_uuid,
            storagepoolID=self._spUUID
        )
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Failed attaching fake storage domain' +
                str(status['status']['message'])
            )

    def _activateFakeStorageDomain(self):
        self._log.info('_activateFakeStorageDomain')
        status = self._cli.activateStorageDomain(
            storagedomainID=self._fake_mastersd_uuid,
            storagepoolID=self._spUUID
        )
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Failed activating fake storage domain' +
                str(status['status']['message'])
            )

    def _destroyFakeStorageDomain(self):
        self._log.info('_destroyFakeStorageDomain')
        sdUUID = self._fake_mastersd_uuid
        status = self._cli.formatStorageDomain(sdUUID)
        if status['status']['code'] != 0:
            raise RuntimeError(status['status']['message'])

    def _detachStorageDomain(self, sdUUID, newMasterSdUUID):
        self._log.info('detachStorageDomain')
        spUUID = self._spUUID
        master_ver = 1
        status = self._cli.detachStorageDomain(
            storagedomainID=sdUUID,
            storagepoolID=spUUID,
            masterSdUUID=newMasterSdUUID,
            masterVersion=master_ver
        )
        if status['status']['code'] != 0:
            raise RuntimeError(status['status']['message'])
        heconflib.task_wait(self._cli, self._log)
        self._log.debug(self._cli.getSpmStatus(spUUID))
        info = self._cli.getStoragePoolInfo(spUUID)
        self._log.debug(info)
        self._log.debug(self._cli.repoStats())

    def _destroyStoragePool(self):
        self._log.info('_destroyStoragePool')
        spUUID = self._spUUID
        ID = self._host_id
        scsi_key = spUUID
        status = self._cli.destroyStoragePool(
            storagepoolID=spUUID,
            hostID=ID,
            scsiKey=scsi_key,
        )
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(status['status']['message'])
        self._spUUID = constants.BLANK_UUID
        self.pool_exists = False

    def _isSPM(self):
        self._log.info('isSPM')
        status = self._cli.getSpmStatus(self._spUUID)
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Unable to check SPM: ' + str(status['status']['message'])
            )
        return status['spmStatus'] == 'SPM'

    def _spmStart(self):
        self._log.info('spmStart')
        if self._isSPM():
            self._log.debug('SPM is already active')
        else:
            prevID = -1
            prevLVER = -1
            scsiFencing = 'false'
            maxHostID = 250
            version = 3
            status = self._cli.spmStart(
                storagepoolID=self._spUUID,
                prevID=prevID,
                prevLver=prevLVER,
                enableScsiFencing=scsiFencing,
                maxHostID=maxHostID,
                domVersion=version,
            )
            self._log.debug(status)
            if status['status']['code'] != 0:
                raise RuntimeError(
                    'Unable to start SPM: ' + str(status['status']['message'])
                )
            count = 0
            limit = 30
            delay = 2
            while not self._isSPM() and count < limit:
                self._log.debug('Waiting for SPM - ' + str(count))
                count += 1
                time.sleep(delay)
            if count == limit and not self._isSPM():
                raise RuntimeError(
                    'SPM didn\'t come up after ' + str(count * delay) +
                    ' seconds'
                )

    def _spmStop(self):
        self._log.info('spmStop')
        if self._isSPM():
            status = self._cli.spmStop(
                self._spUUID,
            )
            self._log.debug(status)
            if status['status']['code'] != 0:
                raise RuntimeError(
                    'Unable to stop SPM: ' + str(status['status']['message'])
                )
        else:
            self._log.debug('SPM is already stopped')

    def _stopMonitoringDomain(self):
        self._log.info('Stop monitoring domain')
        status = self._cli.stopMonitoringDomain(
            sdUUID=self._sdUUID,
        )
        self._log.debug(status)
        rcode = status['status']['code']
        if rcode == 0:
            self._log.debug('Successfully stopped monitoring the domain')
        elif rcode == 900:
            self._log.debug(
                'Failed stopped monitoring the domain '
                'because it\'s not owned by the agent but it''s '
                'safe to continue'
            )
        else:
            raise RuntimeError(
                'Failed stopping monitoring domain: ' +
                str(status['status']['message'])
            )

    def _startMonitoringDomain(self):
        self._log.info('Start monitoring domain')
        status = self._cli.startMonitoringDomain(
            sdUUID=self._sdUUID,
            hostID=self._host_id,
        )
        self._log.debug(status)
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Failed starting monitoring domain: ' +
                str(status['status']['message'])
            )
        heconflib.domain_wait(self._sdUUID, self._cli, self._log)

    def _reconstructMaster(self, master, dom_dict):
        self._log.info('_reconstructMaster')
        master_ver = 1
        status = self._cli.reconstructMaster(
            storagepoolID=self._spUUID,
            hostId=self._host_id,
            name='hosted_engine',
            masterSdUUID=master,
            masterVersion=master_ver,
            domainDict=dom_dict,
            lockRenewalIntervalSec=0,
            leaseTimeSec=0,
            ioOpTimeoutSec=0,
            leaseRetries=0,
        )
        if status['status']['code'] != 0:
            raise RuntimeError(
                'Failed reconstructing master: ' +
                str(status['status']['message'])
            )

    def _remove_storage_pool(self):
        if not self._isStoragePoolConnected():
            self._safeConnectStoragePool(
                master=self._sdUUID,
                dom_dict={self._sdUUID: 'active'},
            )
        self._spmStart()
        if not self._is_storage_pool_attached():
            self._spmStop()
            return False
        self._attach_loopback_device()
        self._connectFakeStorageDomainServer()
        self._createFakeStorageDomain()
        self._attachFakeStorageDomain()
        self._activateFakeStorageDomain()
        self._spmStop()
        self._disconnectStoragePool()
        master = self._fake_mastersd_uuid
        dom_dict = {
            self._fake_mastersd_uuid: 'active',
            self._sdUUID: 'active',
        }
        self._reconstructMaster(master, dom_dict)
        self._connectStoragePool(master, dom_dict)
        self._spmStart()
        if not self._is_storage_pool_attached():
            self._spmStop()
            return False
        self._detachStorageDomain(self._sdUUID, self._fake_mastersd_uuid)
        self._destroyStoragePool()
        self._disconnectFakeStorageDomainServer()
        self._remove_loopback_device()
        self._log.info(
            'Successfully removed the shared pool'
        )
        return True

    def _is_storage_pool_attached(self):
        sdinfo = self._cli.getStorageDomainInfo(self._sdUUID)
        self._log.debug(sdinfo)
        if sdinfo['status']['code'] != 0:
            raise RuntimeError(sdinfo['status']['message'])
        return (
            len(sdinfo['pool']) != 0 and
            sdinfo['pool'][0] == self._spUUID
        )

    def _is_in_engine_maintenance(self):
        splist = self._cli.getConnectedStoragePoolsList()
        self._log.debug(splist)
        if splist['status']['code'] == 0:
            poollist = splist['items'] if 'items' in splist else []
            for pool in poollist:
                if pool != self._spUUID:
                    self._log.info(
                        (
                            'This host is connected to other storage '
                            'pools: {poollist}'
                        ).format(
                            poollist=poollist,
                        )
                    )
                    return False
        else:
            raise RuntimeError(
                'Unable to fetch connected storage pool list: %s' %
                str(splist['status']['message'])
            )

        vmlist = self._cli.list()
        self._log.debug(vmlist)
        if vmlist['status']['code'] == 0:
            runningVM = set(vmlist['items'] if 'items' in vmlist else [])
            otherVM = runningVM - set([self._HEVMID])
            if len(otherVM) > 0:
                self._log.info('Other VMs are running on this host')
                return False
        else:
            raise RuntimeError(
                'Unable to fetch VM list: %s' %
                str(vmlist['status']['message'])
            )

        return True

    def _wrote_updated_conf_file(self, content):
        if content:
            try:
                tmp_conf_file = tempfile.mkstemp(
                    dir=constants.OVIRT_HOSTED_ENGINE_LB_DIR
                )[1]
                self._log.debug(
                    "Writing on temporary file: '{fname}'".format(
                        fname=tmp_conf_file
                    )
                )
                with open(tmp_conf_file, 'w') as f:
                    f.write(content)
                self._log.debug('Moving conf file to its final location')
                if util.isOvirtNode():
                    self._execute(
                        args=(
                            'sudo',
                            '-n',
                            '/usr/sbin/unpersist',
                            constants.ENGINE_SETUP_CONF_FILE,
                        ),
                        raiseOnError=True
                    )
                self._execute(
                    args=(
                        'sudo',
                        '-n',
                        'mv',
                        '-b',
                        '-f',
                        '-Z',
                        tmp_conf_file,
                        constants.ENGINE_SETUP_CONF_FILE,
                    ),
                    raiseOnError=True
                )
                self._log.debug('Fixing conf file attributes')
                self._execute(
                    args=(
                        'sudo',
                        '-n',
                        'chown',
                        'root:root',
                        constants.ENGINE_SETUP_CONF_FILE,
                    ),
                    raiseOnError=True
                )
                self._execute(
                    args=(
                        'sudo',
                        '-n',
                        'chmod',
                        '644',
                        constants.ENGINE_SETUP_CONF_FILE,
                    ),
                    raiseOnError=True
                )
                if util.isOvirtNode():
                    self._execute(
                        args=(
                            'sudo',
                            '-n',
                            '/usr/sbin/persist',
                            constants.ENGINE_SETUP_CONF_FILE,
                        ),
                        raiseOnError=True
                    )
            except (OSError, IOError) as ex:
                msg = "Unable to write updated HE conf file: {ex}".format(
                    ex=str(ex),
                )
                self._log.error(msg)
                raise RuntimeError(msg)
        else:
            msg = 'Unable to write updated HE conf file'
            self._log.error(msg)
            raise RuntimeError(msg)

    def _move_to_shared_conf(self):
        self._log.info('_move_to_shared_conf')

        answer_c = self._get_conffile_content(
            constants.HEConfFiles.HECONFD_ANSWERFILE
        )
        heconf_c = self._get_conffile_content(
            constants.HEConfFiles.HECONFD_HECONF
        )
        brokerconf_c = self._get_conffile_content(
            constants.HEConfFiles.HECONFD_BROKER_CONF
        )
        vmconf_c = self._get_conffile_content(
            constants.HEConfFiles.HECONFD_VM_CONF
        )
        if not self._isStoragePoolConnected():
            self._safeConnectStoragePool(
                master=self._sdUUID,
                dom_dict={self._sdUUID: 'active'},
            )
        self._spmStart()
        if self._is_conf_volume_there():
            self._spmStop()
            return False
        self._create_shared_conf_volume(
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            constants.DEFAULT_CONF_IMAGE_SIZE_GB,
        )
        self._create_conf_tar(
            answer_c,
            heconf_c,
            brokerconf_c,
            vmconf_c,
        )
        self._log.info(
            'Successfully moved the configuration to the shared storage'
        )
        return True

    def fix_storage_path(self):
        """
        Fix storage path in hosted-engine config file
        """
        self._log.info('Fixing storage path in conf file')
        content = self._get_conffile_content(
            constants.HEConfFiles.HECONFD_HECONF,
            self._fix_path_in_conf_file,
        )
        self._wrote_updated_conf_file(content)
        self._log.info('Successfully fixed path in conf file')

    def upgrade_35_36(self):
        uptodate = self.is_conf_file_uptodate()
        if uptodate:
            self._log.info('Host configuration is already up-to-date')
            return False
        else:
            self._log.info('Upgrading to current version')
            if not self._is_in_engine_maintenance():
                self._log.error(
                    'Unable to upgrade while not in maintenance mode: '
                    'please put this host into maintenance mode '
                    'from the engine, and manually restart this service '
                    'when ready'
                )
                return False
            self._stopMonitoringDomain()
            if not self._is_conf_volume_there():
                moved = self._move_to_shared_conf()
                if not moved:
                    self._log.error(
                        'The upgrade procedure was already started '
                        'on another host while I was waiting for the SPM. '
                        'Avoid moving to the shared conf to avoid race '
                        'conditions.'
                    )
            if self._is_storage_pool_attached():
                removed = self._remove_storage_pool()
                if not removed:
                    self._log.error(
                        'The upgrade procedure was already started '
                        'on another host while I was waiting for the SPM. '
                        'Avoid removing the storage pool to avoid race '
                        'conditions.'
                    )
            self._startMonitoringDomain()
            content = self._get_conffile_content(
                constants.HEConfFiles.HECONFD_HECONF,
                self._update_conf_file_35_36,
            )
            self._wrote_updated_conf_file(content)
            self._log.info('Successfully upgraded')
            return True
