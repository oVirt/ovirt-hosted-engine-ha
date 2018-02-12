import os
import os.path

from ovirt_hosted_engine_ha.env.config_file import ConfigFile
from ovirt_hosted_engine_ha.lib import heconflib

from .config_constants import SD_UUID, SP_UUID, \
    CONF_VOLUME_UUID, CONF_IMAGE_UUID


class SharedConfigFile(ConfigFile):
    def __init__(self, id, local_path,
                 sd_config,
                 remote_path=None, writable=False,
                 rawonly=False, logger=None):
        super(SharedConfigFile, self).__init__(id, local_path, writable,
                                               logger=logger)
        self.rawonly = rawonly
        self.sd = sd_config
        self.remote_path = remote_path if remote_path is not None \
            else os.path.basename(self.path)

    def get(self, key, d=None):
        if self.rawonly:
            return None
        else:
            return super(SharedConfigFile, self).get(key, d)

    def download(self, logger=None):
        content = self._get_file_content_from_shared_storage()
        if not content:
            self._logger(logger).debug(
                "unable to get a valid content, "
                "failing back to conf file from a previous release"
            )
            return False

        if self._publish_local_conf_file(content):
            super(SharedConfigFile, self).download()
            return True
        else:
            return False

    def _publish_local_conf_file(self, content):
        with open(self.path, "w") as f:
            f.write(content)
            return True

    def _get_config_volume_path(self):
        sdUUID = self.sd.get(SD_UUID)
        spUUID = self.sd.get(SP_UUID)

        if sdUUID is None or spUUID is None:
            if self._logger:
                self._logger.debug("Configuration image not configured yet")
                return None

        try:
            conf_vol_uuid = self.sd.get(CONF_VOLUME_UUID)
            conf_img_uuid = self.sd.get(CONF_IMAGE_UUID)
        except (KeyError, ValueError):
            if self._logger:
                self._logger.debug("Configuration image doesn't exist")
            return None

        volumepath = heconflib.get_volume_path(
            spUUID,
            sdUUID,
            conf_img_uuid,
            conf_vol_uuid
        )
        return volumepath

    def set_and_write(self, key, value):
        content = self._create_new_content_for_file_on_shared_storage(
            key,
            value
        )
        self._update_content_on_shared_conf(content)

    def _update_content_on_shared_conf(self, file_content):
        """
        writes the new content to the specified file.
        :param file_content: the new content
        """
        volumepath = self._get_config_volume_path()
        heconflib.add_file_to_conf_archive(
            self._logger,
            os.path.basename(self.path),
            file_content,
            volumepath
        )

    def _create_new_content_for_file_on_shared_storage(
            self,
            key_to_set,
            new_value
    ):
        """
        creates the new content for the specified config_type
        with the new value for the given key.
        this method does not update the file.
        :param key_to_set: the key to set
        :param new_value: the value to set
        :return: the updated content of the file
        """
        text = ''
        file_content = self._get_file_content_from_shared_storage()

        for line in file_content.splitlines():
            tokens = line.split('=', 1)
            if len(tokens) > 1:
                key = tokens[0].strip()
                value = tokens[1].strip()
                if key != key_to_set or value == new_value:
                    new_line = line
                else:
                    new_line = '{key}={new_value}'. \
                        format(key=key_to_set, new_value=new_value)
            else:
                new_line = line
            text += '{line}\n'.format(line=new_line)

        return text

    def _get_file_content_from_shared_storage(self):
        config_volume_path = self._get_config_volume_path()
        if not config_volume_path:
            return None

        archive_fname = os.path.basename(self.remote_path)
        if self._logger:
            self._logger.debug(
                "Reading '{archive_fname}' from '{source}'".format(
                    archive_fname=archive_fname,
                    source=config_volume_path,
                )
            )
        content = None
        if heconflib.validateConfImage(self._logger, config_volume_path):
            content = heconflib.extractConfFile(
                self._logger,
                config_volume_path,
                archive_fname,
            )
        return content
