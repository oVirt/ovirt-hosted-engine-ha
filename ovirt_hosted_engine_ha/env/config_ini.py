import configparser

from ovirt_hosted_engine_ha.env.config_shared import SharedConfigFile


class SharedIniFile(SharedConfigFile):
    def __init__(self, id, local_path, sd_config,
                 remote_path=None, writable=False, logger=None):
        super(SharedIniFile, self).__init__(
            id=id,
            local_path=local_path,
            sd_config=sd_config,
            remote_path=remote_path,
            writable=writable,
            rawonly=False,
            logger=logger)
        self._conf = configparser.ConfigParser()

    def _prepare_key(self, key):
        if "." not in key:
            key = "default." + key
        return key.split(".", 1)

    def get(self, key, d=None):
        section, option = self._prepare_key(key)
        if key not in self:
            return d
        return self._conf.get(section, option)

    def load(self):
        # TODO should we clear the current conf first?
        self._conf.read(self.path)

    def write(self, logger=None):
        with open(self.path, "w") as fp:
            self._conf.write(fp)

    def __contains__(self, key):
        section, key = self._prepare_key(key)
        return self._conf.has_option(section, key)

    def set(self, key, val):
        section, key = self._prepare_key(key)
        if not self._conf.has_section(section):
            self._conf.add_section(section)
        self._conf.set(section, key, val)

    def keys(self):
        return [".".join([sec, opt])
                for sec in self._conf.sections()
                for opt in self._conf.options(sec)]

    def _create_new_content_for_file_on_shared_storage(
            self, key_to_set, new_value):
        section, option = self._prepare_key(key_to_set)
        self._conf.set(section, option, new_value)
        self.write()
        return self.raw()
