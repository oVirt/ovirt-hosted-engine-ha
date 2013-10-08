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
CLEANFILES = \
	constants.py \
	$(NULL)

haenvdir = $(engine_ha_libdir)/env

dist_haenv_PYTHON = \
	__init__.py \
	config.py \
	path.py \
	$(NULL)

haenv_PYTHON = \
	constants.py \
	$(NULL)

hastatedir = $(engine_ha_statedir)

dist_hastate_DATA = \
	ha.conf \
	$(NULL)

EXTRA_DIST = \
	constants.py.in \
	$(NULL)

clean-local: \
	python-clean \
	$(NULL)

all-local: \
	$(DISTFILES) \
	python-syntax-check \
	$(NULL)
