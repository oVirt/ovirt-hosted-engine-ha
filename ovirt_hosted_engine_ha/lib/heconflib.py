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


import getpass
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

from vdsm.client import ServerError


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


def _enforce_vdsm_user(cmd_list):
    if not getpass.getuser() == agentconst.VDSM_USER:
        cmd_list = ['sudo', '-u', agentconst.VDSM_USER, ] + cmd_list
    return cmd_list


def _dd_pipe_tar(logger, path, tar_parameters):
    cmd_dd_list = _enforce_vdsm_user([
        'dd',
        'if={source}'.format(source=path),
        'bs=4k',
    ])
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
                logger.error(
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
    rc, stdout, stderr = _dd_pipe_tar(logger, imagepath, ['-xOf', '-', file, ])
    if rc != 0:
        return None
    return stdout


def get_conf_archive_MD5(imagepath):
    cmd_list = _enforce_vdsm_user(['md5sum', imagepath])
    md5_pipe = subprocess.Popen(
        cmd_list,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = md5_pipe.communicate()
    md5_pipe.wait()
    md5_pipe.stdout.close()
    return stdout.split()[0]


def add_file_to_conf_archive(logger, file_name, file_content, volumepath):
    content_by_name = {}
    for f in _CONF_FILES:
        if f == file_name:
            content_by_name[f] = file_content
        else:
            content_by_name[f] = extractConfFile(logger, volumepath, f)
    hash = get_conf_archive_MD5(volumepath)
    temp_archive = _create_temp_archive(
        logger,
        content_by_name[envconst.HEConfFiles.HECONFD_ANSWERFILE],
        content_by_name[envconst.HEConfFiles.HECONFD_HECONF],
        content_by_name[envconst.HEConfFiles.HECONFD_BROKER_CONF],
        content_by_name[envconst.HEConfFiles.HECONFD_VM_CONF]
    )
    new_hash = get_conf_archive_MD5(volumepath)
    if hash == new_hash:
        _save_configuration_archive(logger, temp_archive, volumepath)
    else:
        raise RuntimeError("Update failed. "
                           "The configuration file was changed by somebody "
                           "else, try again.")


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
    temp_archive = _create_temp_archive(
        logger,
        answefile_content,
        heconf_content,
        broker_conf_content,
        vm_conf_content
    )
    _save_configuration_archive(logger, temp_archive, dest)


def _save_configuration_archive(logger, temp_archive, dest):
    if logger:
        logger.debug('saving on: ' + dest)

    cmd_list = _enforce_vdsm_user([
        'dd',
        'if={source}'.format(source=temp_archive),
        'of={dest}'.format(dest=dest),
        'bs=4k',
    ])
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
    os.unlink(temp_archive)
    if pipe.returncode != 0:
        raise RuntimeError('Unable to write HEConfImage')


def _create_temp_archive(logger,
                         answefile_content,
                         heconf_content,
                         broker_conf_content,
                         vm_conf_content):
    # TODO: fix the engine to handle actual archive size from the tar header
    # see: https://bugzilla.redhat.com/show_bug.cgi?id=1336655#c12
    EXPECTED_SIZE = 20480
    tempdir = tempfile.gettempdir()
    fd, _tmp_tar = tempfile.mkstemp(
        suffix='.tar',
        dir=tempdir,
    )
    os.close(fd)
    if logger:
        logger.debug('temp tar file: ' + _tmp_tar)

    # python tarfile seams to ignore bufsize value creating new archive
    # let's create one with system tar and a custom record_size
    # see: https://bugzilla.redhat.com/show_bug.cgi?id=1492157
    cmd_list = [
        'tar',
        '--record-size={record_size}'.format(record_size=EXPECTED_SIZE),
        '-cf',
        '{dest}'.format(dest=_tmp_tar),
        '-T',
        '/dev/null',
    ]
    pipe = subprocess.Popen(
        cmd_list,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    pipe.wait()
    if pipe.returncode != 0:
        raise RuntimeError('Unable to create temporary archive')
    # and open it in append mode
    tar = tarfile.open(name=_tmp_tar, mode='a')

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
    statinfo = os.stat(_tmp_tar)
    if statinfo.st_size != EXPECTED_SIZE:
        raise RuntimeError((
            'Archive size doens\'t match expected size: '
            'actual {a} - expected {e}'
        ).format(
            a=statinfo.st_size,
            e=EXPECTED_SIZE,
        ))
    os.chown(
        _tmp_tar,
        pwd.getpwnam(agentconst.VDSM_USER).pw_uid,
        grp.getgrnam(agentconst.VDSM_GROUP).gr_gid,
    )
    return _tmp_tar


def get_volume_path(type, sd_uuid, img_uuid, vol_uuid):
    """
    Return path of the volume file inside the domain
    :param type: storage type
    :param sd_uuid: StorageDomain UUID
    :param img_uuid: Image UUID
    :param vol_uuid: Volume UUID
    :returns: The local path of the required volume
    """
    volume_path = os.path.join(
        envconst.SD_RUN_DIR,
        sd_uuid,
        img_uuid,
        vol_uuid
    )

    if not os.path.exists(volume_path):
        raise RuntimeError(
            'Path to volume {vol_uuid} not found in {root}'.format(
                vol_uuid=vol_uuid,
                root=envconst.SD_RUN_DIR,
            )
        )
    return volume_path


def task_wait(cli, logger):
    wait = True
    while wait:
        if logger:
            logger.debug('Waiting for existing tasks to complete')

        try:
            statuses = cli.Host.getAllTasksStatuses()
            if logger:
                logger.debug(statuses)
        except ServerError as e:
            raise RuntimeError(
                'Error getting task status: {error}'.format(
                    error=str(e)
                )
            )
        all_completed = True
        taskIDs = set(statuses.keys())
        for taskID in taskIDs:
            if (
                    'taskState' in statuses[taskID] and
                    statuses[taskID]['taskState'] != 'finished'
            ):
                all_completed = False
            else:
                try:
                    cli.Task.clear(taskID=taskID)
                except ServerError:
                    # Ignore error
                    pass

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

        try:
            response = cli.Host.getStats()
            if logger:
                logger.debug(response)
        except ServerError as e:
            if logger:
                logger.error(e)
            raise RuntimeError('Error acquiring VDS status: %s', str(e))

        try:
            domains = response['storageDomains']
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
    try:
        newUUID = cli.Volume.create(
            volumeID=volUUID,
            storagepoolID=spUUID,
            storagedomainID=sdUUID,
            imageID=imgUUID,
            size=str(int(sizeGB) * pow(2, 30)),
            volFormat=volFormat,
            preallocate=preallocate,
            diskType=diskType,
            desc=desc,
            srcImgUUID=envconst.BLANK_UUID,
            srcVolUUID=envconst.BLANK_UUID,
        )
    except ServerError as e:
        raise RuntimeError(str(e))

    if logger:
        logger.debug(
            (
                'Created configuration volume {newUUID}, request was:\n'
                '- image: {imgUUID}\n'
                '- volume: {volUUID}'
            ).format(
                newUUID=newUUID,
                imgUUID=imgUUID,
                volUUID=volUUID,
            )
        )

    task_wait(cli, logger)
    # Expose the image (e.g., activates the lv) on the host (HSM verb).
    if logger:
        logger.debug('configuration volume: prepareImage')

    try:
        path = cli.Image.prepare(
            storagepoolID=spUUID,
            storagedomainID=sdUUID,
            imageID=imgUUID,
            volumeID=volUUID
        )
        if logger:
            logger.debug(path)
    except ServerError as e:
        raise RuntimeError(str(e))

# vim: expandtab tabstop=4 shiftwidth=4
