#
# ovirt-hosted-engine-ha -- ovirt hosted engine high availability
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

AUTOMAKE_OPTIONS = foreign 1.9
ACLOCAL_AMFLAGS = -I build/m4

include build/var_subst.inc

MAINTAINERCLEANFILES = \
	config.log \
	config.status \
	$(srcdir)/ABOUT-NLS \
	$(srcdir)/aclocal.m4 \
	$(srcdir)/autom4te.cache \
	$(srcdir)/config.guess \
	$(srcdir)/config.rpath \
	$(srcdir)/config.sub \
	$(srcdir)/configure \
	$(srcdir)/depcomp \
	$(srcdir)/install-sh \
	$(srcdir)/Makefile.in \
	$(srcdir)/missing \
	$(srcdir)/py-compile \
	$(NULL)
CLEANFILES = \
	ovirt-hosted-engine-ha.spec \
	$(NULL)

EXTRA_DIST = \
	ovirt-hosted-engine-ha.spec.in \
	README.md \
	$(NULL)

SUBDIRS = \
	bin \
	build \
	doc \
	initscripts \
	ovirt_hosted_engine_ha \
	sudoers \
	$(NULL)

dist_noinst_DATA = \
	ovirt-hosted-engine-ha.spec \
	$(NULL)

dist_noinst_SCRIPTS = \
	autogen.sh \
	$(NULL)

TMPREPOS = tmp.repos
srpm:	dist
	rm -fr "$(TMPREPOS)"
	mkdir -p $(TMPREPOS)/{SPECS,RPMS,SRPMS,SOURCES}
	$(RPMBUILD) --define="_topdir `pwd`/$(TMPREPOS)" -ts "$(PACKAGE_TARNAME)-$(PACKAGE_VERSION).tar.gz"
	@echo
	@echo "srpm available at '$(TMPREPOS)'"
	@echo

rpm:	srpm
	$(RPMBUILD) --define="_topdir `pwd`/$(TMPREPOS)" --rebuild "$(TMPREPOS)"/SRPMS/*.src.rpm
	@echo
	@echo "rpm(s) available at '$(TMPREPOS)'"
	@echo

test:
	if [ -n "$(COVERAGE)" ]; then \
		$(COVERAGE) run -m unittest discover -p "*_test.py" ; \
	else \
		$(PYTHON) -m unittest discover -p "*_test.py" ; \
	fi

clean-local:
	rm -fr "$(TMPREPOS)"
	rm -fr target

maintainer-clean-local:
	sed '/^$$/d' "$(srcdir)/m4/.gitignore" | while read f; do \
		rm -f $(srcdir)/m4/$$f; \
	done

fsm:
	PYTHONPATH=. python build/gen_gv.py > doc/agent-fsm.gv
	dot doc/agent-fsm.gv -Tpng -o doc/agent-fsm.png
	PYTHONPATH=. python build/gen_html.py > doc/agent-fsm.html

