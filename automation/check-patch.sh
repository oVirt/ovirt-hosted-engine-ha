#!/bin/bash -e
./autogen.sh --system
if [ `rpm -qv automake` != automake-1.16.1-5.fc29.noarch ]
then
# Workaround for http://bugzilla.redhat.com/1716384
make distcheck
rm -f *.tar.gz
fi

./automation/build-artifacts.sh
