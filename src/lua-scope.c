/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ */
 

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>

#include <glib.h>
#include <glib/gstdio.h> /* got g_stat() */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_LUA_H
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#endif

#include "lua-load-factory.h"
#include "lua-scope.h"
#include "chassis-stats.h"

static int proxy_lua_panic (lua_State *L);

static void *chassis_lua_alloc(void *userdata, void *ptr, size_t osize, size_t nsize);

/**
 * @deprecated will be removed in 1.0
 * @see lua_scope_new()
 */
lua_scope *lua_scope_init(void) {
	return lua_scope_new();
}

lua_scope *lua_scope_new(void) {
	lua_scope *sc;

	sc = g_new0(lua_scope, 1);

#ifdef HAVE_LUA_H
	sc->L = lua_newstate(chassis_lua_alloc, NULL);
	luaL_openlibs(sc->L);
	lua_atpanic(sc->L, proxy_lua_panic);
#endif

	sc->mutex = g_mutex_new();	/*remove lock*/

	return sc;
}

void lua_scope_free(lua_scope *sc) {
	if (!sc) return;

#ifdef HAVE_LUA_H
	/**
	 * enforce that the stack is clean
	 *
	 * we still have items on the stack
	 */
	if (lua_gettop(sc->L) != 0) {
		g_critical("%s: lua-scope has %d items on the stack", 
				G_STRLOC,
				lua_gettop(sc->L));
	}

	/* FIXME: we might want to cleanup the cached-scripts in the registry */

	lua_close(sc->L);
#endif
	g_mutex_free(sc->mutex);	/*remove lock*/

	g_free(sc);
}

void lua_scope_get(lua_scope *sc, const char G_GNUC_UNUSED* pos) {
/*	g_warning("%s: === waiting for lua-scope", pos); */
	g_mutex_lock(sc->mutex);
/*	g_warning("%s: +++ got lua-scope", pos); */
#ifdef HAVE_LUA_H
	sc->L_top = lua_gettop(sc->L);
#endif

	return;
}

void lua_scope_release(lua_scope *sc, const char* pos) {
#ifdef HAVE_LUA_H
	if (lua_gettop(sc->L) != sc->L_top) {
		g_critical("%s: lua-stack out of sync: is %d, should be %d", pos, lua_gettop(sc->L), sc->L_top);
	}
#endif

	g_mutex_unlock(sc->mutex);
/*	g_warning("%s: --- released lua scope", pos); */

	return;
}

#ifdef HAVE_LUA_H
/**
 * load the lua script
 *
 * wraps luaL_loadfile and prints warnings when needed
 *
 * on success we leave a function on the stack, otherwise a error-msg
 *
 * @see luaL_loadfile
 * @returns the lua_State
 */
lua_State *lua_scope_load_script(lua_scope *sc, const gchar *name) {
	lua_State *L = sc->L;
	int stack_top = lua_gettop(L);
	/**
	 * check if the script is in the cache already
	 *
	 * if it is and is fresh, duplicate it
	 * otherwise load it and put it in the cache
	 */
#if 1
	lua_getfield(L, LUA_REGISTRYINDEX, "cachedscripts");         /* sp += 1 */
	if (lua_isnil(L, -1)) {
		/** oops, not there yet */
		lua_pop(L, 1);                                       /* sp -= 1 */

		lua_newtable(L);             /* reg.cachedscripts = { } sp += 1 */
		lua_setfield(L, LUA_REGISTRYINDEX, "cachedscripts"); /* sp -= 1 */
	
		lua_getfield(L, LUA_REGISTRYINDEX, "cachedscripts"); /* sp += 1 */
	}
	g_assert(lua_istable(L, -1)); /** the script-cache should be on the stack now */

	g_assert(lua_gettop(L) == stack_top + 1);

	/**
	 * reg.
	 *   cachedscripts.  <- on the stack
	 *     <name>.
	 *       mtime
	 *       func
	 */

	lua_getfield(L, -1, name);
	if (lua_istable(L, -1)) {
		struct stat st;
		time_t cached_mtime;
		off_t cached_size;

		/** the script cached, check that it is fresh */
		if (0 != g_stat(name, &st)) {
			gchar *errmsg;
			/* stat() failed, ... not good */

			lua_pop(L, 2); /* cachedscripts. + cachedscripts.<name> */

			errmsg = g_strdup_printf("%s: stat(%s) failed: %s (%d)",
				       G_STRLOC, name, g_strerror(errno), errno);
			
			lua_pushstring(L, errmsg);

			g_free(errmsg);

			g_assert(lua_isstring(L, -1));
			g_assert(lua_gettop(L) == stack_top + 1);

			return L;
		}

		/* get the mtime from the table */
		lua_getfield(L, -1, "mtime");
		g_assert(lua_isnumber(L, -1));
		cached_mtime = lua_tonumber(L, -1);
		lua_pop(L, 1);

		/* get the mtime from the table */
		lua_getfield(L, -1, "size");
		g_assert(lua_isnumber(L, -1));
		cached_size = lua_tonumber(L, -1);
		lua_pop(L, 1);

		if (st.st_mtime != cached_mtime || 
		    st.st_size  != cached_size) {
			lua_pushnil(L);
			lua_setfield(L, -2, "func"); /* zap the old function on the stack */

			if (0 != luaL_loadfile_factory(L, name)) {
				/* log a warning and leave the error-msg on the stack */
				g_warning("%s: reloading '%s' failed", G_STRLOC, name);

				/* cleanup a bit */
				lua_remove(L, -2); /* remove the cachedscripts.<name> */
				lua_remove(L, -2); /* remove cachedscripts-table */

				g_assert(lua_isstring(L, -1));
				g_assert(lua_gettop(L) == stack_top + 1);

				return L;
			}
			lua_setfield(L, -2, "func");

			/* not fresh, reload */
			lua_pushinteger(L, st.st_mtime);
			lua_setfield(L, -2, "mtime");   /* t.mtime = ... */

			lua_pushinteger(L, st.st_size);
			lua_setfield(L, -2, "size");    /* t.size = ... */
		}
	} else if (lua_isnil(L, -1)) {
		struct stat st;

		lua_pop(L, 1); /* remove the nil, aka not found */

		/** not known yet */
		lua_newtable(L);                /* t = { } */
		
		if (0 != g_stat(name, &st)) {
			gchar *errmsg;

			/* stat() failed, ... not good */
			errmsg = g_strdup_printf("%s: stat(%s) failed: %s (%d)",
				       G_STRLOC, name, g_strerror(errno), errno);

			lua_pop(L, 2); /* cachedscripts. + cachedscripts.<name> */

			lua_pushstring(L, errmsg);

			g_free(errmsg);

			g_assert(lua_isstring(L, -1));
			g_assert(lua_gettop(L) == stack_top + 1);

			return L;
		}

		if (0 != luaL_loadfile_factory(L, name)) {
			/* leave the error-msg on the stack */

			/* cleanup a bit */
			lua_remove(L, -2); /* remove the t = { } */
			lua_remove(L, -2); /* remove cachedscripts-table */

			g_assert(lua_isstring(L, -1));
			g_assert(lua_gettop(L) == stack_top + 1);

			return L;
		}

		lua_setfield(L, -2, "func");

		lua_pushinteger(L, st.st_mtime);
		lua_setfield(L, -2, "mtime");   /* t.mtime = ... */

		lua_pushinteger(L, st.st_size);
		lua_setfield(L, -2, "size");    /* t.size  = ... */

		lua_setfield(L, -2, name);      /* reg.cachedscripts.<name> = t */

		lua_getfield(L, -1, name);
	} else {
		/* not good */
		lua_pushstring(L, "stack is out of sync");

		g_return_val_if_reached(L);
	}

	/* -- the cache is fresh now, get the script from it */

	g_assert(lua_istable(L, -1));
	lua_getfield(L, -1, "func");
	g_assert(lua_isfunction(L, -1));

	/* cachedscripts and <name> are still on the stack */
#if 0
	g_debug("(load) [-3] %s", lua_typename(L, lua_type(L, -3)));
	g_debug("(load) [-2] %s", lua_typename(L, lua_type(L, -2)));
	g_debug("(load) [-1] %s", lua_typename(L, lua_type(L, -1)));
#endif
	lua_remove(L, -2); /* remove the reg.cachedscripts.<name> */
	lua_remove(L, -2); /* remove the reg.cachedscripts */

	/* create a copy of the script for us:
	 *
	 * f = function () 
	 *   return function () 
	 *     <script> 
	 *   end 
	 * end
	 * f()
	 *
	 * */
	if (0 != lua_pcall(L, 0, 1, 0)) {
		g_warning("%s: lua_pcall(factory<%s>) failed", G_STRLOC, name);

		return L;
	}
#else
	if (0 != luaL_loadfile(L, name)) {
		/* log a warning and leave the error-msg on the stack */
		g_warning("%s: luaL_loadfile(%s) failed", G_STRLOC, name);

		return L;
	}
#endif
	g_assert(lua_isfunction(L, -1));
	g_assert(lua_gettop(L) == stack_top + 1);

	return L;
}

/**
 * dump the content of a lua table
 */
void proxy_lua_dumptable(lua_State *L) {
	g_assert(lua_istable(L, -1));
	
	lua_pushnil(L);
	while (lua_next(L, -2) != 0) {
		int t = lua_type(L, -2);
		
		switch (t) {
			case LUA_TSTRING:
				g_message("[%d] (string) %s", 0, lua_tostring(L, -2));
				break;
			case LUA_TBOOLEAN:
				g_message("[%d] (bool) %s", 0, lua_toboolean(L, -2) ? "true" : "false");
				break;
			case LUA_TNUMBER:
				g_message("[%d] (number) %g", 0, lua_tonumber(L, -2));
				break;
			default:
				g_message("[%d] (%s)", 0, lua_typename(L, lua_type(L, -2)));
				break;
		}
		g_message("[%d] (%s)", 0, lua_typename(L, lua_type(L, -1)));
		
		lua_pop(L, 1);
	}
}

/**
 * dump the types on the lua stack
 */
void proxy_lua_dumpstack(lua_State *L) {
	int i;
	int top = lua_gettop(L);
    if (top == 0) return;
	for (i = 1; i <= top; i++) {
		int t = lua_type(L, i);
		switch (t) {
			case LUA_TSTRING:
				printf("'%s'", lua_tostring(L, i));
				break;
			case LUA_TBOOLEAN:
				printf("%s", lua_toboolean(L, i) ? "true" : "false");
				break;
			case LUA_TNUMBER:
				printf("'%g'", lua_tonumber(L, i));
				break;
			default:
				printf("%s", lua_typename(L, t));
				break;
		}
		printf("  ");
	}
	printf("\n");
}


/**
 * perform a verbose dump of the lua stack
 */
void proxy_lua_dumpstack_verbose(lua_State *L) {
    int i;
    int top = lua_gettop(L);
    GString *stack_desc;
    if (top == 0) {
        fprintf(stderr, "[Empty stack]\n");
        return;
    }
	stack_desc = g_string_sized_new(100);
	for (i = 1; i <= top; i++) {
		int t = lua_type(L, i);
		switch (t) {
			case LUA_TSTRING:
                g_string_append_printf(stack_desc, "[%d] STRING %s\n", i, lua_tostring(L, i));
				break;
			case LUA_TBOOLEAN:
                g_string_append_printf(stack_desc, "[%d] BOOL %s\n", i, lua_toboolean(L, i) ? "true" : "false");
				break;
			case LUA_TNUMBER:
                g_string_append_printf(stack_desc, "[%d] NUMBER %g\n", i, lua_tonumber(L, i));
				break;
			default:
                g_string_append_printf(stack_desc, "[%d] %s <cannot dump>\n", i, lua_typename(L, t));
				break;
		}
	}
	fprintf(stderr, "%s\n", stack_desc->str);
	g_string_free(stack_desc, TRUE);
}

/**
 * print out information about the currently execute lua code
 */
void proxy_lua_currentline(lua_State *L, int level) {
	lua_Debug ar;
	const char *name;
	if (lua_getstack(L, level, &ar)) {
		lua_getinfo(L, "lnS", &ar);
		/* currentline is offset by 1 line because of the
		 * wrapper function we introduce in lua-load-factory.c
		 */
		ar.currentline--;
		name = ar.namewhat[0] == '\0' ? "unknown" : ar.name;
		printf("%s in %s (line %d)\n", name, ar.short_src, ar.currentline);
	} else {
		printf("level %d exceeds the current stack depth\n", level);
	}
}

/**
 * our own shiny panic function for Lua.
 * The auxlib atpanic handler exits the application, which we don't want.
 * Let's crash intentionally so we can get a coredump or be thrown into the debugger
 */
static int proxy_lua_panic (lua_State *L) {
	char *null_ptr = NULL;
	fprintf(stderr, "PANIC: unprotected error in call to Lua API (%s)\nIntentionally crashing this application!\n", lua_tostring(L, -1));
	*null_ptr = 0;
	return 0;
}

/**
 * Our own instrumented version of the lua allocator function.
 * It is handling all malloc/realloc/free cases as described in detail in the Lua reference manual.
 *
 * @param userdata NULL and unused in our case (userdata passed to lua_newstate)
 * @param ptr the pointer to the block to be malloced/realloced/freed
 * @param osize the original size of the block
 * @param nsize the requested size of the block
 */
static void* chassis_lua_alloc(void G_GNUC_UNUSED *userdata, void *ptr, size_t osize, size_t nsize) {
	gpointer p;
	gint cur_size;

	/* the free case */
	if (nsize == 0) {
		if (osize != 0) {
			CHASSIS_STATS_FREE_INC_NAME(lua_mem);
			CHASSIS_STATS_ADD_NAME(lua_mem_bytes, -osize);
			g_free(ptr);
		}
		return NULL;
	} 
	/* track the maximum of the mem-usage inside lua
	 *
	 * g_atomic_... works against signed integers, but we actually would need something bigger to be safe
	 *
	 * stats may be wrong if the lua-mem-* counters actually go about MAX_INT 
	 */
	if (osize == 0) { 		/* the plain malloc case */
		CHASSIS_STATS_ALLOC_INC_NAME(lua_mem);
		CHASSIS_STATS_ADD_NAME(lua_mem_bytes, nsize);
		
		cur_size = CHASSIS_STATS_GET_NAME(lua_mem_bytes);
		if (cur_size > CHASSIS_STATS_GET_NAME(lua_mem_bytes_max)) {
			CHASSIS_STATS_SET_NAME(lua_mem_bytes_max, cur_size);
		}
		return g_malloc(nsize);
	} 

	p = g_realloc(ptr, nsize);

	if (!p) return p;
	
	CHASSIS_STATS_ADD_NAME(lua_mem_bytes, nsize - osize); /* might be negative if Lua tries to shrink something */

	cur_size = CHASSIS_STATS_GET_NAME(lua_mem_bytes);
	if (cur_size > CHASSIS_STATS_GET_NAME(lua_mem_bytes_max)) {
		CHASSIS_STATS_SET_NAME(lua_mem_bytes_max, cur_size);
	}
	
	return p;
}


#endif

