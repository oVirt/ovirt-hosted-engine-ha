#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
# Copyright (C) 2015 Red Hat, Inc.
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

PYTHON_SYNTAX_CHECK_FILES := \
	$(srcdir)/__init__.py \
	$(srcdir)/ovf2VmParams.py \
	$(srcdir)/ovf2VmParams_test.py \
	$(NULL)

MAINTAINERCLEANFILES = \
	$(srcdir)/Makefile.in \
	$(NULL)

ovflibdir = $(engine_ha_libdir)/lib/ovf

dist_ovflib_PYTHON = \
	__init__.py \
	ovf2VmParams.py \
	ovfenvelope.py \
	ovf_store.py \
	$(NULL)

dist_noinst_PYTHON = \
	ovf2VmParams_test.py \
	$(NULL)

dist_noinst_DATA = \
	ovf_test.xml \
	ovf_test_max_vcpu.xml \
	ovf_test_v4.2.xml \
	ovf_test_v4.2_unsafe.xml \
	$(NULL)

clean-local: \
	python-clean \
	$(NULL)

all-local: \
	$(DISTFILES) \
	python-syntax-check \
	$(NULL)
