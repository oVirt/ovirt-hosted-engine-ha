# ovirt-hosted-engine-ha -- ovirt hosted engine high availability

[![Copr build status](https://copr.fedorainfracloud.org/coprs/ovirt/ovirt-master-snapshot/package/ovirt-hosted-engine-ha/status_image/last_build.png)](https://copr.fedorainfracloud.org/coprs/ovirt/ovirt-master-snapshot/package/ovirt-hosted-engine-ha/)

Copyright (C) 2013 Red Hat, Inc.

In order to build the project, the following dependencies are needed:
 - autoconf
 - automake
 - all dependencies in .spec file's BuildRequires section

To build:
```bash
 $ ./autogen.sh --system
```

 Then choose one of the following:
 ```bash
 $ make         # compile only
 $ make install # compile and install
 $ make dist    # compile and create distribution tarball
 $ make rpm     # compile and create rpm
```

Cleanup:
```bash
 $ make distclean  # clean files from compilation and automake
```
 -or-
```bash
 $ make clean      # clean only files from compilation
```
Patches are welcome: gerrit.ovirt.org/ovirt-hosted-engine-ha

Before sending one, please configure the commit template:
```bash
 # from project root directory
 $ git config commit.template commit-template.txt
```
