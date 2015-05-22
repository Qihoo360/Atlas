#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <stdio.h>

int main(int argc, char *argv[]) {
	lua_State *L;
	int i;

	if (argc < 2) return -1;

	L = luaL_newstate();
	luaL_openlibs(L);

	lua_newtable(L);
	for (i = 0; i + 1 < argc; i++) {
		lua_pushstring(L, argv[i + 1]);
		lua_rawseti(L, -2, i);
	}
	lua_setglobal(L, "arg");

	if (luaL_dofile(L, argv[1])) {
		fprintf(stderr, "%s: %s", argv[0], lua_tostring(L, -1));
		return -1;
	}


	lua_close(L);
	return 0;
}
