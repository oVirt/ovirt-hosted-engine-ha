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
	agent-log.conf \
	constants.py \
	$(NULL)

SUBDIRS = \
	$(NULL)

agentdir = $(engine_ha_libdir)/agent

dist_agent_PYTHON = \
	__init__.py \
	agent.py \
	hosted_engine.py \
	$(NULL)

agent_PYTHON = \
	constants.py \
	$(NULL)

agentconfdir = $(engine_ha_confdir)

agentconf_DATA = \
	agent.conf \
	agent-log.conf \
	$(NULL)

EXTRA_DIST = \
	agent.conf \
	agent-log.conf.in \
	constants.py.in \
	$(NULL)

clean-local: \
	python-clean \
	$(NULL)

all-local: \
	$(DISTFILES) \
	python-syntax-check \
	$(NULL)
