import logging
import fcntl
import os.path


class ConfigFile(object):
    def __init__(self, id, local_path, writable=False,
                 mandatory=False, logger=None, optional_keys=None):
        self.id = id
        self.readonly = not writable
        self.path = local_path
        self._conf = {}
        self.mandatory = mandatory
        self._parent_logger = logger
        self._optional_keys = optional_keys \
            if optional_keys is not None else []

    def __str__(self):
        return "<%s id:%s path:%s ro:%s>" %\
               (type(self), self.id, self.path, self.readonly)

    @property
    def _logger(self):
        if self._parent_logger is None:
            return logging.getLogger(__name__)
        else:
            return self._parent_logger.getChild("config." + self.id)

    def present(self):
        return os.path.exists(self.path)

    def download(self):
        """Get the current version of the conf from shared storage
           and save a cache to self.path."""
        pass

    def raw(self):
        with open(self.path, 'r') as f:
            return f.read()

    def load(self):
        """Load the config values from self.path"""
        self._conf = {}

        try:
            with open(self.path, 'r') as f:
                for line in f:
                    tokens = line.split('=', 1)
                    if len(tokens) > 1:
                        self._conf[tokens[0].strip()] = tokens[1].strip()
            return self._conf
        except (OSError, IOError) as ex:
            log = self._logger
            level = log.error
            level("Configuration file '%s' not available [%s]", self.path, ex)
            return {}

    def write(self, logger=None):
        """Write the current configuration to local file."""
        if self.readonly:
            raise Exception("Read only config file %s"
                            " does not support writes." % self.path)

        if logger:
            logger.debug(
                "Writing to '{target}'".format(
                    target=self.path,
                )
            )

        return True

    def set_and_write(self, key, value):
        """Update a configuration value and persist it."""

        with open(self.path, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)

            # self._load_config_files() can re-open the exclusively-locked
            # file because it's being called from the same process as the
            # lock holder
            self.download()
            self.load()
            self._conf[key] = str(value)

            text = ''
            for k, v in self._conf.iteritems():
                text += '{k}={v}\n'.format(k=k, v=v)

            f.write(text)
            f.truncate()

    def get(self, key, d=None):
        return self._conf.get(key, d)

    def has_key(self, key):
        return key in self

    def set(self, key, val):
        self._conf[key] = val

    def keys(self):
        return self._conf.keys()

    def optional_keys(self):
        return self._optional_keys

    def __contains__(self, item):
        return item in self._conf or item in self._optional_keys

    def __getitem__(self, item):
        return self.get(item)


class ConfigFileWithFallback(ConfigFile):
    """Search for a key in multiple config files. The first one
       to contain it wins."""

    def __init__(self, id, sources, mandatory=False, logger=None):
        super(ConfigFileWithFallback, self).__init__(id, "",
                                                     writable=True,
                                                     mandatory=mandatory,
                                                     logger=logger)
        self._sources = sources

    def present(self):
        return any([x.present() for x in self._sources])

    def raw(self):
        raise Exception("This is a composite configuration file.")

    def get(self, key, d=None):
        for x in self._sources:
            if key in x:
                return x.get(key)
        else:
            return d

    def set(self, key, val):
        raise Exception("This is a composite configuration file.")

    def set_and_write(self, key, val):
        raise Exception("This is a composite configuration file.")

    def __contains__(self, item):
        return any([item in x for x in self._sources])
