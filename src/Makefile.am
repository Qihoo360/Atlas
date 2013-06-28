#  $%BEGINLICENSE%$
#  Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
# 
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License as
#  published by the Free Software Foundation; version 2 of the
#  License.
# 
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
#  02110-1301  USA
# 
#  $%ENDLICENSE%$
luaextdir = ${pkglibdir}/lua
plugindir = ${pkglibdir}/plugins


BUILD_CPPFLAGS = $(LUA_CFLAGS) $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(GMODULE_CFLAGS) $(GTHREAD_CFLAGS)
BUILD_CFLAGS   = -DLUAEXTDIR="\"$(luaextdir)\""  -DPLUGINDIR="\"$(plugindir)\""
BUILD_LDADD    = $(GLIB_LIBS) $(GMODULE_LIBS) libmysql-chassis.la $(GTHREAD_LIBS) libmysql-proxy.la -L$(luaextdir) libsql-tokenizer.la

BUILT_SOURCES =
if USE_WRAPPER_SCRIPT
## we are self-contained
## put all the binaries into a "hidden" location, the wrapper scripts are in ./scripts/
libexec_PROGRAMS = mysql-binlog-dump mysql-proxy mysql-myisam-dump
else
bin_PROGRAMS            = mysql-binlog-dump mysql-myisam-dump mysql-proxy
endif

mysql_proxy_SOURCES		= mysql-proxy-cli.c
mysql_proxy_CPPFLAGS	= $(BUILD_CPPFLAGS)
mysql_proxy_CFLAGS		= $(BUILD_CFLAGS)
mysql_proxy_LDADD		= $(BUILD_LDADD)

mysql_binlog_dump_SOURCES	= mysql-binlog-dump.c
mysql_binlog_dump_CPPFLAGS	= $(BUILD_CPPFLAGS)
mysql_binlog_dump_CFLAGS	= $(BUILD_CFLAGS)
mysql_binlog_dump_LDADD		= $(BUILD_LDADD)

mysql_myisam_dump_SOURCES	= mysql-myisam-dump.c
mysql_myisam_dump_CPPFLAGS	= $(BUILD_CPPFLAGS)
mysql_myisam_dump_CFLAGS	= $(BUILD_CFLAGS)
mysql_myisam_dump_LDADD		= $(BUILD_LDADD)

lib_LTLIBRARIES = 

# functionality extending what's currently in glib
lib_LTLIBRARIES += libmysql-chassis-glibext.la
libmysql_chassis_glibext_la_SOURCES = \
	glib-ext.c \
	glib-ext-ref.c

libmysql_chassis_glibext_la_LDFLAGS  = -export-dynamic -no-undefined -dynamic
libmysql_chassis_glibext_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(GMODULE_CFLAGS) -ggdb
libmysql_chassis_glibext_la_LIBADD   = $(GLIB_LIBS)

# library implementing timing functionality
lib_LTLIBRARIES += libmysql-chassis-timing.la
libmysql_chassis_timing_la_SOURCES = \
	chassis-timings.c \
	chassis-gtimeval.c \
	my_rdtsc.c

libmysql_chassis_timing_la_LDFLAGS  = -export-dynamic -no-undefined -dynamic
libmysql_chassis_timing_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -ggdb
libmysql_chassis_timing_la_LIBADD   = $(GLIB_LIBS) libmysql-chassis-glibext.la
if USE_SUNCC_ASSEMBLY
libmysql_chassis_timing_la_CPPFLAGS += \
	$(top_srcdir)/src/my_timer_cycles.il
endif

## the sql-tokenizer library

lib_LTLIBRARIES += libsql-tokenizer.la
libsql_tokenizer_la_SOURCES = \
	../lib/sql-tokenizer-keywords.c \
	../lib/sql-tokenizer.l \
	../lib/sql-tokenizer-tokens.c 

../lib/sql-tokenizer.c:../lib/sql-tokenizer-keywords.c

../lib/sql-tokenizer-keywords.c: sql-tokenizer-gen
	./sql-tokenizer-gen > ../lib/sql-tokenizer-keywords.c

DISTCLEANFILES = \
	sql-tokenizer.c

noinst_PROGRAMS=sql-tokenizer-gen
sql_tokenizer_gen_SOURCES=\
	../lib/sql-tokenizer-tokens.c \
	../lib/sql-tokenizer-gen.c
sql_tokenizer_gen_CPPFLAGS=${GLIB_CFLAGS} -I${srcdir} 
sql_tokenizer_gen_LDADD=${GLIB_LIBS}




libsql_tokenizer_la_LDFLAGS = -export-dynamic -no-undefined -dynamic
libsql_tokenizer_la_CPPFLAGS = $(MYSQL_CFLAGS) $(EVENT_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) $(GTHREAD_CFLAGS) -I../lib/
libsql_tokenizer_la_LIBADD   = $(EVENT_LIBS)   $(GLIB_LIBS)   $(LUA_LIBS) $(GMODULE_LIBS) $(GTHREAD_LIBS) libmysql-chassis-timing.la libmysql-chassis-glibext.la



## the core library
lib_LTLIBRARIES += libmysql-chassis.la
libmysql_chassis_la_SOURCES = \
	lua-load-factory.c \
	lua-scope.c \
	chassis-plugin.c \
	chassis-log.c \
	chassis-mainloop.c \
	chassis-event-thread.c \
	chassis-keyfile.c \
	chassis-path.c \
	chassis-filemode.c \
	chassis-limits.c \
	chassis-shutdown-hooks.c \
	chassis-stats.c \
	chassis-frontend.c \
	chassis-options.c \
	chassis-unix-daemon.c \
	chassis-win32-service.c


libmysql_chassis_la_LDFLAGS  = -export-dynamic -no-undefined -dynamic
libmysql_chassis_la_CPPFLAGS = $(MYSQL_CFLAGS) $(EVENT_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) $(GTHREAD_CFLAGS) -ggdb
libmysql_chassis_la_LIBADD   = $(EVENT_LIBS)   $(GLIB_LIBS)   $(LUA_LIBS) $(GMODULE_LIBS) $(GTHREAD_LIBS) libmysql-chassis-timing.la libmysql-chassis-glibext.la

lib_LTLIBRARIES += libmysql-proxy.la
libmysql_proxy_la_SOURCES = \
	network-mysqld.c \
	network-mysqld-lua.c \
	network-mysqld-proto.c \
	network-mysqld-binlog.c \
	network-mysqld-packet.c \
	network_mysqld_type.c \
	network_mysqld_proto_binary.c \
	network-mysqld-masterinfo.c \
	network-conn-pool.c  \
	network-conn-pool-lua.c  \
	network-queue.c \
	network-socket.c \
	network-socket-lua.c \
	network-address.c \
	network-address-lua.c \
	network-injection.c \
	network-injection-lua.c \
	network-backend.c \
	network-backend-lua.c \
	lua-env.c

libmysql_proxy_la_LDFLAGS  = -export-dynamic -no-undefined -dynamic
libmysql_proxy_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -ggdb
libmysql_proxy_la_LIBADD   = $(EVENT_LIBS) $(GLIB_LIBS) $(GMODULE_LIBS) libmysql-chassis.la libmysql-chassis-timing.la libmysql-chassis-glibext.la

## should be packaged, but not installed
noinst_HEADERS=\
	network-debug.h

include_HEADERS=\
	network-mysqld.h \
	network-mysqld-lua.h \
	network-mysqld-proto.h \
	network-mysqld-binlog.h \
	network-mysqld-packet.h \
	network_mysqld_type.h \
	network_mysqld_proto_binary.h \
	network-mysqld-masterinfo.h \
	network-conn-pool.h \
	network-conn-pool-lua.h \
	network-queue.h \
	network-socket.h \
	network-socket-lua.h \
	network-address.h \
	network-address-lua.h \
	sys-pedantic.h \
	chassis-plugin.h \
	chassis-log.h \
	chassis-keyfile.h \
	chassis-mainloop.h \
	chassis-path.h \
	chassis-filemode.h \
	chassis-limits.h \
	chassis-event-thread.h \
	chassis-gtimeval.h \
	glib-ext.h \
	glib-ext-ref.h \
	string-len.h \
	lua-load-factory.h \
	lua-scope.h \
	lua-env.h \
	network-injection.h \
	network-injection-lua.h \
	chassis-shutdown-hooks.h \
	chassis-exports.h \
	network-exports.h \
	network-backend.h \
	network-backend-lua.h \
	disable-dtrace.h \
	lua-registry-keys.h \
	chassis-stats.h \
	chassis-timings.h \
	chassis-frontend.h \
	chassis-options.h \
	chassis-win32-service.h \
	chassis-unix-daemon.h \
	my_rdtsc.h

if ENABLE_DTRACE

BUILT_SOURCES += proxy-dtrace-provider.h
nodist_libmysql_proxy_la_SOURCES = proxy-dtrace-provider.h
CLEANFILES = proxy-dtrace-provider.h

proxy-dtrace-provider.h: Makefile
	$(DTRACE) -h -s $(top_srcdir)/src/proxy-dtrace-provider.d -o $(top_builddir)/src/proxy-dtrace-provider.h

## Solaris DTrace needs to perform some link time operations at probe sites.
## On OS X this is performed automagically (and dtrace(1M) doesn't even have -G)
if OS_SOLARIS

proxy-dtrace-provider.o: proxy-dtrace-provider.h $(libmysql_proxy_la_OBJECTS)
	$(DTRACE) -G -s $(top_srcdir)/src/proxy-dtrace-provider.d -o $(top_builddir)/src/proxy-dtrace-provider.o \
	    $(libmysql_proxy_la_OBJECTS:%.lo=.libs/%.o)


libmysql_proxy_la_LIBADD += proxy-dtrace-provider.o
endif

endif

## for now test-latency uses macros with var-args which isn't supported on
## some compilers. Include the source in tar.gz, but don't build it by default
# noinst_PROGRAMS=test-latency 
# test_latency_SOURCES=test-latency.c
# test_latency_CPPFLAGS= $(MYSQL_INCLUDE) $(GLIB_CFLAGS)
# test_latency_LDADD= $(MYSQL_LIBS) $(GLIB_LIBS)

EXTRA_DIST=test-latency.c proxy-dtrace-provider.d CMakeLists.txt my_timer_cycles.il
