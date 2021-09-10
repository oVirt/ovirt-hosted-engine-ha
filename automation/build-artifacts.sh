#!/bin/bash -xe
[[ -d exported-artifacts ]] \
|| mkdir -p exported-artifacts

[[ -d tmp.repos ]] \
|| mkdir -p tmp.repos

SUFFIX=".$(date -u +%Y%m%d%H%M%S).git$(git rev-parse --short HEAD)"

# mock runner is not setting up the system correctly
# https://issues.redhat.com/browse/CPDEVOPS-242
readarray -t pkgs < automation/build-artifacts.packages
dnf install -y "${pkgs[@]}"

if [ -x /usr/bin/python3 ] ; then
export PYTHON=/usr/bin/python3
fi

./autogen.sh --system
make dist
yum-builddep ovirt-hosted-engine-ha.spec
rpmbuild \
    -D "_topdir $PWD/tmp.repos" \
    -D "release_suffix ${SUFFIX}" \
    -ta ovirt-hosted-engine-ha-*.tar.gz

mv ./*.tar.gz exported-artifacts
find \
    "$PWD/tmp.repos" \
    -iname \*.rpm \
    -exec mv {} exported-artifacts/ \;
