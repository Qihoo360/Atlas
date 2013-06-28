TESTS=proxy-test.lua \
      active-queries.lua \
      proxy-auto-config.lua \
      proxy-commands.lua \
      proxy-tokenizer.lua \
      proxy-parser.lua \
      mysql-proto.lua \
      chassis.lua

TESTS_ENVIRONMENT = \
	LUA_PATH="${top_srcdir}/lib/?.lua" \
	LUA_CPATH=".libs/?.@DYNLIB_LUA_SUFFIX@;${top_builddir}/lib/.libs/?.@DYNLIB_LUA_SUFFIX@" \
	@DYNLIB_PATH_VAR@="${top_builddir}/src/.libs/:${@DYNLIB_PATH_VAR@}" \
	${builddir}/lua-runner


check_PROGRAMS=lua-runner

lua_runner_SOURCES=lua_runner.c
lua_runner_LDFLAGS=${LUA_LDFLAGS}
lua_runner_LDADD=${LUA_LIBS}
lua_runner_CFLAGS=${LUA_CFLAGS}

EXTRA_DIST=$(TESTS) CMakeLists.txt
