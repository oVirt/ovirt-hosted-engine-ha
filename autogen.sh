#!/bin/sh
#
# ovirt-hosted-engine-ha
# Copyright (C) 2013 Red Hat, Inc.
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

# you probably need:
# - autoconf
# - automake
# - gettext-devel

autoreconf -ivf

if test "x$1" = "x--system"; then
    shift
    prefix=/usr
    exec_prefix=/usr
    #libdir=$prefix/lib
    sysconfdir=/etc
    localstatedir=/var
    #if [ -d /usr/lib64 ]; then
    #  libdir=$prefix/lib64
    #fi
    #EXTRA_ARGS="--prefix=$prefix --sysconfdir=$sysconfdir --localstatedir=$localstatedir --libdir=$libdir"
    EXTRA_ARGS="--prefix=$prefix --exec_prefix=$exec_prefix --sysconfdir=$sysconfdir --localstatedir=$localstatedir"
    echo "Running ./configure with $EXTRA_ARGS $@"
else
    if test -z "$*" && test ! -f "$THEDIR/config.status"; then
        echo "I am going to run ./configure with no arguments - if you wish "
        echo "to pass any to it, please specify them on the $0 command line."
    fi
fi

if test -z "$*" && test -f config.status; then
    ./config.status --recheck
else
    ./configure $EXTRA_ARGS "$@"
fi && make clean && {
    echo
    echo "Now type 'make' to compile ovirt-hosted-engine-ha"
}
