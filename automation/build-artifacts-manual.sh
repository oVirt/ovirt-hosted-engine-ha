#!/bin/bash -xe
[[ -d exported-artifacts ]] \
|| mkdir -p exported-artifacts

[[ -d tmp.repos ]] \
|| mkdir -p tmp.repos

rm -rf output

if [ -x /usr/bin/python3 ] ; then
export PYTHON=/usr/bin/python3
fi

./autogen.sh --system
./configure

# Run rpmbuild, assuming the tarball is in the project's directory
rpmbuild \
    -D "_topmdir $PWD/tmp.repos" \
    -D "_srcrpmdir $PWD/output" \
    -ts ovirt-hosted-engine-ha-*.tar.gz

yum-builddep $PWD/output/ovirt-hosted-engine-ha*.src.rpm

rpmbuild \
    -D "_topmdir $PWD/tmp.repos" \
    -D "_rpmdir $PWD/output" \
    --rebuild $PWD/output/ovirt-hosted-engine-ha-*.src.rpm

mv *.tar.gz exported-artifacts
find \
    "$PWD/output" \
    -iname \*.rpm \
    -exec mv {} exported-artifacts/ \;
