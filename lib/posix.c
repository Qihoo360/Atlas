/* Copyright (C) 2008 MySQL AB */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <unistd.h>

#ifdef HAVE_PWD_H
#include <pwd.h>
#endif

#ifdef HAVE_SIGNAL_H
#include <signal.h>
#endif

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

static int lua_getpid (lua_State *L) {
	lua_pushinteger (L, getpid());

	return 1;
}

static int lua_getuid (lua_State *L) {
	lua_pushinteger (L, getuid());

	return 1;
}

#ifdef HAVE_SIGNAL_H
static int lua_kill (lua_State *L) {
	pid_t pid = luaL_checkinteger (L, 1);
	int sig = luaL_checkinteger (L, 2);

	lua_pushinteger(L, kill(pid, sig));

	return 1;
}
#endif

#ifdef HAVE_PWD_H
static int lua_getpwuid(lua_State *L) {
	struct passwd *p;
	int uid = luaL_checkinteger (L, 1);

	if (NULL == (p = getpwuid( (uid_t)uid )) ) {
		lua_pushnil(L);
		return 1;
	}

	lua_newtable (L);
	lua_pushstring (L, "name");
	lua_pushstring (L, p->pw_name);
	lua_settable (L, -3);
	lua_pushstring (L, "uid");
	lua_pushinteger (L, p->pw_uid);
	lua_settable (L, -3);
	lua_pushstring (L, "gid");
	lua_pushinteger (L, p->pw_gid);
	lua_settable (L, -3);
	lua_pushstring (L, "dir");
	lua_pushstring (L, p->pw_dir);
	lua_settable (L, -3);
	lua_pushstring (L, "shell");
	lua_pushstring (L, p->pw_shell);
	lua_settable (L, -3);

	return 1;
}
#endif

/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (C) 2008-2010 Oracle Inc");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "export posix-functions as posix.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaPosix 0.1");
	lua_settable (L, -3);
}


static const struct luaL_reg posixlib[] = {
	{"getpid", lua_getpid},
	{"getuid", lua_getuid},
#ifdef HAVE_PWD_H
	{"getpwuid", lua_getpwuid},
#endif
#ifdef HAVE_SIGNAL_H
	{"kill", lua_kill},
#endif
	{NULL, NULL},
};

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_posix (lua_State *L) {
	luaL_register (L, "posix", posixlib);
	set_info (L);
	return 1;
}

