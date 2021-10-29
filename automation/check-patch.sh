#!/bin/bash -xe

if [ -x /usr/bin/python3 ] ; then
export PYTHON=/usr/bin/python3
fi

echo -e "----  Test packaging and RPM building   ----\n"
# make distcheck skipped due to bug afflicting automake.
# fc29: https://bugzilla.redhat.com/1716384
# fc30: https://bugzilla.redhat.com/1757854
# el8:  https://bugzilla.redhat.com/1759942
# make distcheck
# rm -f *.tar.gz

./automation/build-artifacts.sh

echo -e "----  Test RPM dependencies   ----\n"

# Restoring sane yum environment screwed up by mock-runner
rm -f /etc/yum.conf
dnf reinstall -y system-release dnf dnf-conf
sed -i -re 's#^(reposdir *= *).*$#\1/etc/yum.repos.d#' '/etc/dnf/dnf.conf'
echo "deltarpm=False" >> /etc/dnf/dnf.conf
rm -f /etc/yum/yum.conf
# remove python-coverage if already there.
dnf remove -y platform-python-coverage || true

# test dependencies
dnf install -y https://resources.ovirt.org/pub/yum-repo/ovirt-release-master-tested.rpm
dnf install --downloadonly ./exported-artifacts/*noarch.rpm

echo -e "\n\n----  Test code   ----\n"

# mock runner is not setting up the system correctly
# https://issues.redhat.com/browse/CPDEVOPS-242
readarray -t pkgs < automation/check-patch.packages
dnf install -y "${pkgs[@]}"

# test dependencies
dnf install -y vdsm-client vdsm-python vdsm-jsonrpc

./autogen.sh --system
make check
make test

coverage html -d exported-artifacts/coverage_html_report --rcfile="${PWD}/automation/coverage.rc"
cp automation/index.html exported-artifacts/
