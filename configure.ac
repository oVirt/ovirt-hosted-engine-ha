dnl
dnl ovirt-hosted-engine-ha -- ovirt hosted engine high availability
dnl Copyright (C) 2013-2018 Red Hat, Inc.
dnl
dnl This library is free software; you can redistribute it and/or
dnl modify it under the terms of the GNU Lesser General Public
dnl License as published by the Free Software Foundation; either
dnl version 2.1 of the License, or (at your option) any later version.
dnl
dnl This library is distributed in the hope that it will be useful,
dnl but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
dnl Lesser General Public License for more details.
dnl
dnl You should have received a copy of the GNU Lesser General Public
dnl License along with this library; if not, write to the Free Software
dnl Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
dnl
AC_PREREQ(2.60)

define([VERSION_MAJOR], [2])
define([VERSION_MINOR], [5])
define([VERSION_FIX], [2])
define([VERSION_NUMBER], VERSION_MAJOR[.]VERSION_MINOR[.]VERSION_FIX)
define([VERSION_RELEASE], [0.0])
define([VERSION_SUFFIX], [master.][m4_esyscmd([date -u +%Y%m%d%H%M%S | tr -d '\n'])])

AC_INIT([ovirt-hosted-engine-ha], VERSION_NUMBER[-]VERSION_RELEASE[.]VERSION_SUFFIX)
PACKAGE_RPM_VERSION="VERSION_NUMBER"
PACKAGE_RPM_RELEASE="VERSION_RELEASE.VERSION_SUFFIX"
AC_SUBST([PACKAGE_RPM_VERSION])
AC_SUBST([PACKAGE_RPM_RELEASE])

AC_CONFIG_AUX_DIR([build/build-aux])
AC_CONFIG_MACRO_DIR([build/m4])
AC_CONFIG_SRCDIR([ovirt_hosted_engine_ha/broker/__init__.py])
AM_INIT_AUTOMAKE([1.9 tar-pax])

AC_ARG_ENABLE(
	[python-syntax-check],
	[AS_HELP_STRING([--disable-python-syntax-check], [disable python syntax check @<:@default=enabled@:>@])],
	,
	[enable_python_syntax_check="yes"]
)

AC_PROG_INSTALL
AC_PROG_SED
AC_PROG_MAKE_SET
AC_PROG_MKDIR_P
AC_PROG_LN_S

AC_ARG_VAR([PYFLAKES], [path to pyflakes utility])
AC_CHECK_PROGS([PYFLAKES], [pyflakes])
AC_ARG_VAR([PEP8], [path to pep8 utility])
AC_CHECK_PROGS([PEP8], [pycodestyle-3 pep8])
AC_ARG_VAR([COVERAGE], [path to coverage utility])
AC_CHECK_PROGS([COVERAGE], [coverage])
AC_ARG_VAR([RPMBUILD], [path to rpmbuild utility])
AC_CHECK_PROGS([RPMBUILD], [rpmbuild])

AM_PATH_PYTHON([3.6],, [AC_MSG_ERROR([Cannot find python])])

AC_PATH_PROG([SERVICE_PATH], [service], [/sbin/service])

# User/group must match those in vdsm package
AC_SUBST([VDSM_USER], [vdsm])
AC_SUBST([VDSM_GROUP], [kvm])

AC_SUBST([engine_ha_bindir], ['${exec_prefix}/libexec/${PACKAGE_NAME}'])
AC_SUBST([engine_ha_confdir], ['${sysconfdir}/${PACKAGE_NAME}'])
AC_SUBST([engine_ha_libdir], ['${pythondir}/ovirt_hosted_engine_ha'])
AC_SUBST([engine_ha_logdir], ['${localstatedir}/log/${PACKAGE_NAME}'])
AC_SUBST([engine_ha_rundir], ['${localstatedir}/run/${PACKAGE_NAME}'])
AC_SUBST([engine_ha_statedir], ['${localstatedir}/lib/${PACKAGE_NAME}'])
AC_SUBST([engine_setup_bindir], ['${sbindir}'])

AM_CONDITIONAL([PYTHON_SYNTAX_CHECK], [test "${enable_python_syntax_check}" = "yes"])

AC_CONFIG_FILES([
	Makefile
	bin/Makefile
	bin/ovirt-ha-agent
	bin/ovirt-ha-broker
	build/Makefile
	doc/Makefile
	initscripts/Makefile
	ovirt_hosted_engine_ha/Makefile
	ovirt_hosted_engine_ha/agent/Makefile
	ovirt_hosted_engine_ha/broker/Makefile
	ovirt_hosted_engine_ha/broker/notifications/Makefile
	ovirt_hosted_engine_ha/broker/submonitors/Makefile
	ovirt_hosted_engine_ha/broker/test/Makefile
	ovirt_hosted_engine_ha/client/Makefile
	ovirt_hosted_engine_ha/env/Makefile
	ovirt_hosted_engine_ha/lib/Makefile
	ovirt_hosted_engine_ha/lib/fsm/Makefile
	ovirt_hosted_engine_ha/lib/ovf/Makefile
	ovirt_hosted_engine_ha/lib/debug/Makefile
	sudoers/Makefile
])
AC_OUTPUT
