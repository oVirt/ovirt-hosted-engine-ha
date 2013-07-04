#
# Copyright 2008-2012 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Refer to the README and COPYING files for full details of the license
#

SUFFIXES: .in

# Reference:
# http://www.gnu.org/software/automake/manual/html_node/Scripts.html
PATHSUBST = sed \
	-e "s,[@]ENGINE_HA_BINDIR[@],$(engine_ha_bindir),g" \
	-e "s,[@]ENGINE_HA_CONFDIR[@],$(engine_ha_confdir),g" \
	-e "s,[@]ENGINE_HA_LIBDIR[@],$(engine_ha_libdir),g" \
	-e "s,[@]ENGINE_HA_LOGDIR[@],$(engine_ha_logdir),g" \
	-e "s,[@]ENGINE_HA_RUNDIR[@],$(engine_ha_rundir),g" \
	-e "s,[@]ENGINE_SETUP_BINDIR[@],$(engine_setup_bindir),g"

CONFIGSUBST = $(top_builddir)/config.status --file=-

%: %.in
	@echo "  SED $@"; $(PATHSUBST) $< |$(CONFIGSUBST) >$@
