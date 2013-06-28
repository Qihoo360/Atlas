plugindir = ${pkglibdir}/plugins

plugin_LTLIBRARIES = libdebug.la
libdebug_la_LDFLAGS  = -export-dynamic -no-undefined -avoid-version -dynamic
libdebug_la_SOURCES  = debug-plugin.c
libdebug_la_LIBADD   = $(EVENT_LIBS) $(GLIB_LIBS) $(GMODULE_LIBS) $(top_builddir)/src/libmysql-proxy.la
libdebug_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -I$(top_srcdir)/src/

EXTRA_DIST=CMakeLists.txt

