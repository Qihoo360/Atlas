#include <sys/time.h>
#include <lua.h>
#include "lua-env.h"

int proxy_tick(lua_State *L)
{
    struct timeval tp; 
    gettimeofday(&tp, 0);
	lua_pushnumber(L, tp.tv_sec);
	lua_pushnumber(L, tp.tv_usec);
	return 2;
}

static const struct luaL_reg time_ticklib[] =
{
		{"tick", proxy_tick},
		{NULL, NULL},
};

extern int luaopen_time_ticker(lua_State *L)
{
		luaL_register(L, "ticker", time_ticklib);
		return 1;
}
