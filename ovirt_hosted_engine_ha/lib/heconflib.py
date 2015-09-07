#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2015 Red Hat, Inc.
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


""" HEConf lib"""


import glob
import grp
import os
import pwd
import subprocess
import tarfile
import tempfile
import time

from io import StringIO

from ovirt_hosted_engine_ha.agent import constants as agentconst
from ovirt_hosted_engine_ha.env import constants as envconst


_CONF_FILES = [
    envconst.HEConfFiles.HECONFD_VERSION,
    envconst.HEConfFiles.HECONFD_ANSWERFILE,
    envconst.HEConfFiles.HECONFD_HECONF,
    envconst.HEConfFiles.HECONFD_BROKER_CONF,
    envconst.HEConfFiles.HECONFD_VM_CONF,
]


def _add_to_tar(tar, fname, content):
    value = StringIO(unicode(content))
    info = tarfile.TarInfo(name=fname)
    value.seek(0, os.SEEK_END)
    info.size = value.tell()
    value.seek(0, os.SEEK_SET)
    tar.addfile(tarinfo=info, fileobj=value)


def _dd_pipe_tar(logger, path, tar_parameters):
    cmd_dd_list = [
        'sudo',
        '-u',
        agentconst.VDSM_USER,
        'dd',
        'if={source}'.format(source=path),
        'bs=4k',
    ]
    cmd_tar_list = ['tar', ]
    cmd_tar_list.extend(tar_parameters)
    if logger:
        logger.debug("executing: '{cmd}'".format(cmd=' '.join(cmd_dd_list)))
        logger.debug("executing: '{cmd}'".format(cmd=' '.join(cmd_tar_list)))
    dd_pipe = subprocess.Popen(
        cmd_dd_list,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_pipe = subprocess.Popen(
        cmd_tar_list,
        stdin=dd_pipe.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Allow dd_pipe to receive a SIGPIPE if tar_pipe exits.
    dd_pipe.stdout.close()
    stdout, stderr = tar_pipe.communicate()
    tar_pipe.wait()
    if logger:
        logger.debug('stdout: ' + str(stdout))
        logger.debug('stderr: ' + str(stderr))
    return tar_pipe.returncode, stdout, stderr


def validateConfImage(logger, imagepath):
    """
    Validates the HE configuration image
    :param logger: a logging instance, None if not needed
    :param imagepath: the path of the HE configuration image on your system
                      it can be obtained with
                      get_volume_path
    :returns: True if valid, False otherwise
    :type: Boolean
    """
    rc, stdout, stderr = _dd_pipe_tar(logger, imagepath, ['-tvf', '-', ])
    if rc != 0:
        return False
    for f in _CONF_FILES:
        if f not in stdout:
            if logger:
                logger.debug(
                    "'{f}' is not stored in the HE configuration image".format(
                        f=f
                    )
                )
            return False
    return True


def extractConfFile(logger, imagepath, file):
    """
    Extracts a single configuration file from the HE configuration image
    :param logger: a logging instance, None if not needed
    :param imagepath: the path of the HE configuration image on your system
                      it can be obtained with
                      get_volume_path
    :param file: the file you ar asking for; valid values are:
                 envconst.HEConfFiles.HECONFD_VERSION,
                 envconst.HEConfFiles.HECONFD_ANSWERFILE,
                 envconst.HEConfFiles.HECONFD_HECONF,
                 envconst.HEConfFiles.HECONFD_BROKER_CONF,
                 envconst.HEConfFiles.HECONFD_VM_CONF
    :returns: The content of the file you asked for, None on errors
    :type: String or None
    """
    if logger:
        logger.debug(
            "extracting '{file}' from '{imagepath}'".format(
                file=file,
                imagepath=imagepath,
            )
        )
    if file not in _CONF_FILES:
        if logger:
            logger.debug(
                "'{file}' is not in the HE configuration image".format(
                )
            )
        return None
    rc, stdout, stderr = _dd_pipe_tar(logger, imagepath, ['-xOf', '-', file, ])
    if rc != 0:
        return None
    return stdout


def create_heconfimage(
        logger,
        answefile_content,
        heconf_content,
        broker_conf_content,
        vm_conf_content,
        dest,
):
    """
    Re-Creates the whole HE configuration image on the specified path
    :param logger: a logging instance, None if not needed
    :param answefile_content: the whole content of
                              envconst.HEConfFiles.HECONFD_ANSWERFILE
                              as a String
    :param heconf_content: the whole content of
                           envconst.HEConfFiles.HECONFD_HECONF
                           as a String
    :param broker_conf_content: the whole content of
                                envconst.HEConfFiles.HECONFD_BROKER_CONF
                                as a String
    :param vm_conf_content: the whole content of
                            envconst.HEConfFiles.HECONFD_VM_CONF
                            as a String
    :param dest: the path of the HE configuration image on your system
                 it can be obtained with
                 get_volume_path
    """
    tempdir = tempfile.gettempdir()
    fd, _tmp_tar = tempfile.mkstemp(
        suffix='.tar',
        dir=tempdir,
    )
    os.close(fd)
    if logger:
        logger.debug('temp tar file: ' + _tmp_tar)
    tar = tarfile.TarFile(name=_tmp_tar, mode='w')
    _add_to_tar(
        tar,
        envconst.HEConfFiles.HECONFD_VERSION,
        agentconst.PACKAGE_VERSION,
    )
    _add_to_tar(
        tar,
        envconst.HEConfFiles.HECONFD_ANSWERFILE,
        answefile_content,
    )
    _add_to_tar(
        tar,
        envconst.HEConfFiles.HECONFD_HECONF,
        heconf_content,
    )
    _add_to_tar(
        tar,
        envconst.HEConfFiles.HECONFD_BROKER_CONF,
        broker_conf_content,
    )
    _add_to_tar(
        tar,
        envconst.HEConfFiles.HECONFD_VM_CONF,
        vm_conf_content,
    )
    tar.close()
    os.chown(
        _tmp_tar,
        pwd.getpwnam(agentconst.VDSM_USER).pw_uid,
        grp.getgrnam(agentconst.VDSM_GROUP).gr_gid,
    )

    if logger:
        logger.debug('saving on: ' + dest)

    cmd_list = [
        'sudo',
        '-u',
        agentconst.VDSM_USER,
        'dd',
        'if={source}'.format(source=_tmp_tar),
        'of={dest}'.format(dest=dest),
        'bs=4k',
    ]
    if logger:
        logger.debug("executing: '{cmd}'".format(cmd=' '.join(cmd_list)))
    pipe = subprocess.Popen(
        cmd_list,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = pipe.communicate()
    pipe.wait()
    if logger:
        logger.debug('stdout: ' + str(stdout))
        logger.debug('stderr: ' + str(stderr))
    os.unlink(_tmp_tar)
    if pipe.returncode != 0:
        raise RuntimeError('Unable to write HEConfImage')


def get_volume_path(type, sd_uuid, img_uuid, vol_uuid):
    """
    Return path of the volume file inside the domain
    :param type: storage type
    :param sd_uuid: StorageDomain UUID
    :param img_uuid: Image UUID
    :param vol_uuid: Volume UUID
    :returns: The local path of the required volume
    """
    volume_path = envconst.SD_MOUNT_PARENT
    if type == 'glusterfs':
        volume_path = os.path.join(
            volume_path,
            'glusterSD',
        )
    volume_path = os.path.join(
        volume_path,
        '*',
        sd_uuid,
        'images',
        img_uuid,
        vol_uuid,
    )
    volumes = glob.glob(volume_path)
    if not volumes:
        raise RuntimeError(
            'Path to volume {vol_uuid} not found in {root}'.format(
                vol_uuid=vol_uuid,
                root=envconst.SD_MOUNT_PARENT,
            )
        )
    return volumes[0]


def task_wait(cli, logger):
    wait = True
    while wait:
        if logger:
            logger.debug('Waiting for existing tasks to complete')
        statuses = cli.getAllTasksStatuses()
        code = statuses['status']['code']
        message = statuses['status']['message']
        if code != 0:
            raise RuntimeError(
                'Error getting task status: {error}'.format(
                    error=message
                )
            )
        tasksStatuses = statuses['allTasksStatus']
        all_completed = True
        for taskID in tasksStatuses:
            if tasksStatuses[taskID]['taskState'] != 'finished':
                all_completed = False
            else:
                cli.clearTask(taskID)
        if all_completed:
            wait = False
        else:
            time.sleep(1)


def domain_wait(sdUUID, cli, logger):
    POLLING_INTERVAL = 5
    acquired = False
    while not acquired:
        time.sleep(POLLING_INTERVAL)
        if logger:
            logger.debug('Waiting for domain monitor')
        response = cli.getVdsStats()
        if logger:
            logger.debug(response)
        if response['status']['code'] != 0:
            if logger:
                logger.debug(response['status']['message'])
            raise RuntimeError('Error acquiring VDS status')
        try:
            domains = response['info']['storageDomains']
            acquired = domains[sdUUID]['acquired']
        except KeyError:
            if logger:
                logger.debug(
                    'Error getting VDS status',
                    exc_info=True,
                )
            raise RuntimeError('Error acquiring VDS status')


def create_and_prepare_image(
        logger,
        cli,
        volFormat,
        preallocate,
        sdUUID,
        spUUID,
        imgUUID,
        volUUID,
        diskType,
        sizeGB,
        desc
):
    # creates a volume on the storage (SPM verb)
    status = cli.createVolume(
        sdUUID,
        spUUID,
        imgUUID,
        str(int(sizeGB) * pow(2, 30)),
        volFormat,
        preallocate,
        diskType,
        volUUID,
        desc,
    )
    if logger:
        logger.debug(status)

    if status['status']['code'] != 0:
        raise RuntimeError(status['status']['message'])
    else:
        if logger:
            logger.debug(
                (
                    'Created configuration volume {newUUID}, request was:\n'
                    '- image: {imgUUID}\n'
                    '- volume: {volUUID}'
                ).format(
                    newUUID=status['status']['message'],
                    imgUUID=imgUUID,
                    volUUID=volUUID,
                )
            )

    task_wait(cli, logger)
    # Expose the image (e.g., activates the lv) on the host (HSM verb).
    if logger:
        logger.debug('configuration volume: prepareImage')
    response = cli.prepareImage(
        spUUID,
        sdUUID,
        imgUUID,
        volUUID
    )
    if logger:
        logger.debug(response)
    if response['status']['code'] != 0:
        raise RuntimeError(response['status']['message'])


# vim: expandtab tabstop=4 shiftwidth=4
