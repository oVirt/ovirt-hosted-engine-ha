import time
from ovirt_hosted_engine_ha.env.config_shared import SharedConfigFile
from ovirt_hosted_engine_ha.lib import monotonic
from ovirt_hosted_engine_ha.lib.ovf import ovf_store
from ovirt_hosted_engine_ha.lib.ovf import ovf2VmParams
from ovirt_hosted_engine_ha.lib import log_filter

LF_OVF_NOT_THERE = 'ovf-not-there'
LF_OVF_EXTRACTION_FAILED = 'ovf-extraction-failed'
LF_OVF_CONVERSION_FAILED = 'ovf-conversion-failed'
LF_OVF_LOG_DELAY = 300


class OvfConfigFile(SharedConfigFile):
    def __init__(self, id, local_path, sd_config,
                 legacy_vm_conf, remote_path=None,
                 writable=False, logger=None):
        super(OvfConfigFile, self).__init__(id, local_path,
                                            sd_config=sd_config,
                                            remote_path=remote_path,
                                            writable=writable,
                                            logger=logger)
        self.legacy_vm_conf = legacy_vm_conf
        self.vm_conf_refresh_time = 0
        self.vm_conf_refresh_time_epoch = 0

    def _get_vm_conf_content_from_ovf_store(self):
        if self._logger:
            self._logger.debug(
                "Trying to get a fresher copy of vm configuration "
                "from the OVF_STORE"
            )

        ovfs = ovf_store.OVFStore()

        if not ovfs.have_store_info():
            try:
                ovfs.scan()
            except (EnvironmentError, Exception) as err:
                self._logger.error(
                    "Failed scanning for OVF_STORE due to %s",
                    err
                )

        if ovfs.have_store_info():
            heovf = ovfs.getEngineVMOVF()
            if heovf is not None:
                self._logger.debug(
                    "Found an OVF for HE VM, "
                    "trying to convert"
                )
                conf = ovf2VmParams.confFromOvf(heovf)
                if conf is not None:
                    self._logger.debug('Got vm.conf from OVF_STORE')
                    return conf
                else:
                    self._logger.error(
                        'Failed converting vm.conf from the VM OVF, '
                        'falling back to initial vm.conf',
                        extra=log_filter.lf_args(
                            LF_OVF_CONVERSION_FAILED,
                            LF_OVF_LOG_DELAY)
                    )
            else:
                self._logger.error(
                    'Failed extracting VM OVF from the OVF_STORE '
                    'volume, falling back to initial vm.conf',
                    extra=log_filter.lf_args(
                        LF_OVF_EXTRACTION_FAILED,
                        LF_OVF_LOG_DELAY)
                )
                # This error might indicate the OVF location changed
                # and clearing the cache will trigger a rescan
                # next time we access the OVF.
                ovfs.clear_store_info()
        else:
            self._logger.error(
                'Unable to identify the OVF_STORE volume, '
                'falling back to initial vm.conf. Please '
                'ensure you already added your first data '
                'domain for regular VMs',
                extra=log_filter.lf_args(
                    LF_OVF_NOT_THERE,
                    LF_OVF_LOG_DELAY
                )
            )
        return None

    def download(self, logger=None):
        header_comment = (
            '# Editing the hosted engine VM is only possible via'
            ' the manager UI\API\n'
            '# This file was generated at {}\n'
            '\n'
        ).format(time.asctime())
        print(header_comment)

        if self._logger:
            self._logger.debug(
                "Reloading vm.conf from the shared storage domain"
            )

        content_from_ovf = self._get_vm_conf_content_from_ovf_store()
        if content_from_ovf:
            content = content_from_ovf
        else:
            self.legacy_vm_conf.download()
            content = self.legacy_vm_conf.raw()

        if content:
            content = "%s%s" % (header_comment, content)
            if self._publish_local_conf_file(content):
                mtime = monotonic.time()
                self.vm_conf_refresh_time = int(mtime)
                self.vm_conf_refresh_time_epoch = int(time.time() - mtime)
                self.load()
