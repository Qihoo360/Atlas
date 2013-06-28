plugindir = ${pkglibdir}/plugins

plugin_LTLIBRARIES = libproxy.la
libproxy_la_LDFLAGS  = -export-dynamic -no-undefined -avoid-version -dynamic
libproxy_la_SOURCES  = proxy-plugin.c
libproxy_la_LIBADD   = $(EVENT_LIBS) $(GLIB_LIBS) $(GMODULE_LIBS) $(top_builddir)/src/libmysql-proxy.la
libproxy_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -I$(top_srcdir)/src/
noinst_HEADERS = proxy-plugin.h

EXTRA_DIST=CMakeLists.txt

