#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2012-2013 Red Hat, Inc.
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

include $(top_srcdir)/build/python.inc
include $(top_srcdir)/build/var_subst.inc

MAINTAINERCLEANFILES = \
	$(srcdir)/Makefile.in \
	$(NULL)
DISTCLEANFILES = \
	config.log \
	$(NULL)
CLEANFILES = \
	broker-log.conf \
	constants.py \
	$(NULL)

SUBDIRS = \
	submonitors \
	notifications \
	test \
	$(NULL)

brokerdir = $(engine_ha_libdir)/broker

dist_broker_PYTHON = \
	__init__.py \
	broker.py \
	listener.py \
	monitor.py \
	notifications.py \
	storage_broker.py \
	submonitor_base.py \
	$(NULL)

broker_PYTHON = \
	constants.py \
	$(NULL)

brokerlogconfdir = $(engine_ha_confdir)

brokerlogconf_DATA = \
	broker-log.conf \
	$(NULL)

brokerconfdir = $(engine_ha_statedir)

brokerconf_DATA = \
	broker.conf \
	$(NULL)


EXTRA_DIST = \
    broker.conf \
	broker-log.conf.in \
	constants.py.in \
	$(NULL)

clean-local: \
	python-clean \
	$(NULL)

all-local: \
	$(DISTFILES) \
	python-syntax-check \
	$(NULL)
