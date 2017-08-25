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

MAINTAINERCLEANFILES = \
	$(srcdir)/Makefile.in \
	$(NULL)

halibdir = $(engine_ha_libdir)/lib

dist_halib_PYTHON = \
	__init__.py \
	brokerlink.py \
	exceptions.py \
	image.py \
	log_filter.py \
	metadata.py \
	heconflib.py \
	storage_backends.py \
	storage_backends_test.py \
	storage_server.py \
	util.py \
	monotonic.py \
	engine.py \
	unixrpc.py \
	upgrade.py \
	$(NULL)

SUBDIRS = \
	fsm \
	ovf \
	$(NULL)

clean-local: \
	python-clean \
	$(NULL)

all-local: \
	$(DISTFILES) \
	python-syntax-check \
	$(NULL)
