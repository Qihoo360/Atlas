plugindir = ${pkglibdir}/plugins

plugin_LTLIBRARIES = libreplicant.la
libreplicant_la_LDFLAGS  = -export-dynamic -no-undefined -avoid-version -dynamic
libreplicant_la_SOURCES  = replicant-plugin.c
libreplicant_la_LIBADD   = $(EVENT_LIBS) $(GLIB_LIBS) $(GMODULE_LIBS) $(top_builddir)/src/libmysql-proxy.la
libreplicant_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -I$(top_srcdir)/src/

EXTRA_DIST=CMakeLists.txt

