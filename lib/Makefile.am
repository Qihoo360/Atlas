luaextdir = ${pkglibdir}/lua

SUBDIRS = proxy 

## those are in the end examples or proof-of-concepts
example_scripts = \
		active-queries.lua \
		active-transactions.lua \
		admin-sql.lua \
		analyze-query.lua \
		auditing.lua \
		commit-obfuscator.lua \
		commit-obfuscator.msc \
		histogram.lua \
		load-multi.lua \
		ro-balance.lua \
		ro-pooling.lua \
		rw-splitting.lua \
		xtab.lua
EXTRA_DIST = 

if USE_WRAPPER_SCRIPT
## only install them if we are self-contained
## otherwise let the packager decide where to put them
dist_doc_DATA = ${example_scripts}
else
EXTRA_DIST += ${example_scripts}
endif

dist_luaext_DATA = \
	admin.lua \
	rw-splitting.lua

luaext_LTLIBRARIES = lfs.la glib2.la chassis.la mysql.la lpeg.la posix.la crc32.la time.la

lfs_la_SOURCES  = lfs.c
## get libtool to build a shared-lib
lfs_la_CPPFLAGS = ${LUA_CFLAGS}
lfs_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version

posix_la_SOURCES  = posix.c
## get libtool to build a shared-lib
posix_la_CPPFLAGS = ${LUA_CFLAGS}
posix_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version

glib2_la_SOURCES  = glib2.c
## get libtool to build a shared-lib
glib2_la_CPPFLAGS = ${LUA_CFLAGS} ${GLIB_CFLAGS}
glib2_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version
glib2_la_LIBADD   = $(GLIB_LIBS)

chassis_la_SOURCES  = chassis.c
## get libtool to build a shared-lib
chassis_la_CPPFLAGS = ${LUA_CFLAGS} ${GLIB_CFLAGS} -I${top_srcdir}/src/
chassis_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version
chassis_la_LIBADD   = ${GLIB_LIBS} ${top_builddir}/src/libmysql-chassis.la

crc32_la_SOURCES  = \
	crc32.c \
	crc32-lua.c
## get libtool to build a shared-lib
crc32_la_CPPFLAGS = ${LUA_CFLAGS} ${GLIB_CFLAGS} -I${top_srcdir}/src/
crc32_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version
crc32_la_LIBADD   = $(GLIB_LIBS)

mysql_la_SOURCES  = \
	mysql-proto.c \
	mysql-password.c \
	sql-tokenizer.l \
	sql-tokenizer-tokens.c \
	sql-tokenizer-keywords.c \
	sql-tokenizer-lua.c 
## get libtool to build a shared-lib
mysql_la_CPPFLAGS = ${LUA_CFLAGS} ${GLIB_CFLAGS} -I${top_srcdir}/src/ ${MYSQL_CFLAGS} -I${top_builddir}/lib/
mysql_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version
mysql_la_LIBADD   = ${GLIB_LIBS} ${top_builddir}/src/libmysql-proxy.la

time_la_SOURCES  = \
	time-ticker-lua.c 
## get libtool to build a shared-lib
time_la_CPPFLAGS = ${LUA_CFLAGS} -I${top_srcdir}/src/
time_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version

lpeg_la_SOURCES  = lpeg.c
lpeg_la_CPPFLAGS = ${LUA_CFLAGS}
lpeg_la_LDFLAGS  = $(AM_LDFLAGS) -module -avoid-version


noinst_HEADERS = \
	sql-tokenizer.h  \
	sql-tokenizer-keywords.h \
	crc32.h

EXTRA_DIST += \
	glib2.def \
	lfs.def \
	chassis.def \
	lpeg.def \
	mysql.def \
	posix.def \
	CMakeLists.txt

DISTCLEANFILES = \
	sql-tokenizer.c \
	sql-tokenizer-keywords.c

noinst_PROGRAMS=sql-tokenizer-gen

sql_tokenizer_gen_SOURCES=\
	sql-tokenizer-gen.c \
	sql-tokenizer-tokens.c

sql_tokenizer_gen_CPPFLAGS=${GLIB_CFLAGS} -I${srcdir}
sql_tokenizer_gen_LDADD=${GLIB_LIBS}

sql-tokenizer.c: sql-tokenizer-keywords.c

sql-tokenizer-keywords.c: sql-tokenizer-gen
	${builddir}/sql-tokenizer-gen > ${builddir}/sql-tokenizer-keywords.c
