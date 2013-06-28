/* $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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


#include <string.h>
 

/**
 * expose the chassis functions into the lua space
 * Also moves the global print function to the 'os' table and
 * replaces print with our logging function at a log level equal to
 * the current chassis minimum log level, so that we always see the
 * output of scripts.
 */


#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <glib.h>

#include "chassis-mainloop.h"
#include "chassis-plugin.h"
#include "chassis-stats.h"
#include "lua-registry-keys.h"

static int lua_chassis_set_shutdown (lua_State G_GNUC_UNUSED *L) {
	chassis_set_shutdown();

	return 0;
}

/**
 * helper function to set GHashTable key, value pairs in a Lua table
 * assumes to have a table on top of the stack.
 */
static void chassis_stats_setluaval(gpointer key, gpointer val, gpointer userdata) {
    const gchar *name = key;
    const guint value = GPOINTER_TO_UINT(val);
    lua_State *L = userdata;

    g_assert(lua_istable(L, -1));
    lua_checkstack(L, 2);

    lua_pushstring(L, name);
    lua_pushinteger(L, value);
    lua_settable(L, -3);
}

/**
 * Expose the plugin stats hashes to Lua for post-processing.
 *
 * Lua parameters: plugin name to fetch stats for (or "chassis" for only getting the global ones)
 *                 might be omitted, then this function gets stats for all plugins, including the chassis
 * Lua return values: nil if the plugin is not loaded
 *                    a table with the stats when given one plugin name
 *                    a table with the plugin names as keys and their values as subtables, the chassis global stats are keyed as "chassis"
 */
static int lua_chassis_stats(lua_State *L) {
    const char *plugin_name = NULL;
    chassis *chas = NULL;
    chassis_plugin *plugin = NULL;
    guint i = 0;
    gboolean found_stats = FALSE;
    int nargs = lua_gettop(L);

    if (nargs == 0) {
        plugin_name = NULL;
    } else if (nargs == 1) {        /* grab only the stats we were asked to fetch */
        plugin_name = luaL_checkstring(L, 1);
    } else {
        return luaL_argerror(L, 2, "currently only zero or one arguments are allowed");
    }
    lua_newtable(L);    /* the table for the stats, either containing sub tables or the stats for the single plugin requested */

    /* retrieve the chassis stored in the registry */
    lua_getfield(L, LUA_REGISTRYINDEX, CHASSIS_LUA_REGISTRY_KEY);
    chas = (chassis*) lua_topointer(L, -1);
    lua_pop(L, 1);

    /* get the global chassis stats */
    if (nargs == 0 && chas) {
        GHashTable *stats_hash = chassis_stats_get(chas->stats);
        if (stats_hash == NULL) {
            found_stats = FALSE;
        } else {
            found_stats = TRUE;

            lua_newtable(L);
            g_hash_table_foreach(stats_hash, chassis_stats_setluaval, L);
            lua_setfield(L, -2, "chassis");
            g_hash_table_destroy(stats_hash);
        }
    }

    if (chas && chas->modules) {
        for (i = 0; i < chas->modules->len; i++) {
            plugin = chas->modules->pdata[i];
            if (plugin->stats != NULL && plugin->get_stats != NULL) {
                GHashTable *stats_hash = NULL;
                
                if (plugin_name == NULL) {
                    /* grab all stats and key them by plugin name */
                    stats_hash = plugin->get_stats(plugin->stats);
                    if (stats_hash != NULL) {
                        found_stats = TRUE;
                    }
                    /* the per-plugin table */
                    lua_newtable(L);
                    g_hash_table_foreach(stats_hash, chassis_stats_setluaval, L);
                    lua_setfield(L, -2, plugin->name);
                    
                    g_hash_table_destroy(stats_hash);
                    
                } else if (g_ascii_strcasecmp(plugin_name, "chassis") == 0) {
                  /* get the global chassis stats */
                    stats_hash = chassis_stats_get(chas->stats);
                    if (stats_hash == NULL) {
                        found_stats = FALSE;
                        break;
                    }
                    found_stats = TRUE;

                    g_hash_table_foreach(stats_hash, chassis_stats_setluaval, L);
                    g_hash_table_destroy(stats_hash);
                    break;
                } else if (g_ascii_strcasecmp(plugin_name, plugin->name) == 0) {
                    /* check for the correct name and get the stats */
                    stats_hash = plugin->get_stats(plugin->stats);
                    if (stats_hash == NULL) {
                        found_stats = FALSE;
                        break;
                    }
                    found_stats = TRUE;
                    
                    /* the table to use is already on the stack */
                    g_hash_table_foreach(stats_hash, chassis_stats_setluaval, L);
                    g_hash_table_destroy(stats_hash);
                    break;
                }
            }
        }
    }
    /* can also be FALSE if we couldn't find the chassis */
    if (!found_stats) {
        lua_pop(L, 1);  /* pop the unused stats table */
        lua_pushnil(L);
        return 1;
    }
    return 1;
}

/**
 * Log a message via the chassis log facility instead of using STDOUT.
 * This is more expensive than just printing to STDOUT, but generally logging
 * in a script would be protected by an 'if(debug)' or be important enough to
 * warrant the extra CPU cycles.
 *
 * Lua parameters: loglevel (first), message (second)
 */
static int lua_chassis_log(lua_State *L) {
    static const char *const log_names[] = {"error", "critical",
        "warning", "message", "info", "debug", NULL};
	static const int log_levels[] = {G_LOG_LEVEL_ERROR, G_LOG_LEVEL_CRITICAL,
        G_LOG_LEVEL_WARNING, G_LOG_LEVEL_MESSAGE,
        G_LOG_LEVEL_INFO, G_LOG_LEVEL_DEBUG};

    int option = luaL_checkoption(L, 1, "message", log_names);
	const char *log_message = luaL_optstring(L, 2, "nil");
	const char *source = NULL;
	const char *first_source = "unknown";
	int currentline = -1;
	int first_line = -1;
	int stackdepth = 1;
	lua_Debug ar;
	chassis *chas;
	
	/* try to get some information about who logs this message */
	do {
		/* walk up the stack to try to find a file name */
        if (!lua_getstack(L, stackdepth, &ar)) break;
        if (!lua_getinfo(L, "Sl", &ar)) break;

		currentline = ar.currentline;
        source = ar.source;
		/* save the first short_src we have encountered,
		   in case we exceed our max stackdepth to check
		 */
		if (stackdepth == 1) {
			first_source = ar.short_src;
			first_line = ar.currentline;
		}
		/* below: '@' comes from Lua's dofile, our lua-load-factory doesn't set it when we load a file. */
	} while (++stackdepth < 11 && source && source[0] != '/' && source[0] != '@'); /* limit walking the stack to a sensible value */

	if (source) {
		if (source[0] == '@') {
			/* skip Lua's "this is from a file" indicator */
			source++;
		}
        lua_getfield(L, LUA_REGISTRYINDEX, CHASSIS_LUA_REGISTRY_KEY);
        chas = (chassis*) lua_topointer(L, -1);
        lua_pop(L, 1);
        if (chas && chas->base_dir) {
            if (g_str_has_prefix(source, chas->base_dir)) {
                source += strlen(chas->base_dir);
                /* skip a leading dir separator */
                if (source[0] == G_DIR_SEPARATOR) source++;
            }
        }
	}
    g_log(G_LOG_DOMAIN, log_levels[option], "(%s:%d) %s", (source ? source : first_source),
			(source ? currentline : first_line), log_message);
	
	return 0;
}

/**
 * these wrapper functions insert the appropriate log level into the stack
 * and then simply call lua_chassis_log()
 */
#define CHASSIS_LUA_LOG(level) static int lua_chassis_log_ ## level(lua_State *L) {\
	int n = lua_gettop(L);\
	int retval;\
	lua_pushliteral(L, #level);\
	lua_insert(L, 1);\
	retval = lua_chassis_log(L);\
	lua_remove(L, 1);\
	g_assert(n == lua_gettop(L));\
	return retval;\
}

CHASSIS_LUA_LOG(error)
CHASSIS_LUA_LOG(critical)
CHASSIS_LUA_LOG(warning)
/* CHASSIS_LUA_LOG(message)*/
CHASSIS_LUA_LOG(info)
CHASSIS_LUA_LOG(debug)

#undef CHASSIS_LUA_LOG


static int lua_chassis_log_message(lua_State *L) {
	int n = lua_gettop(L);
	int retval;
	lua_pushliteral(L, "message");
	lua_insert(L, 1);
	retval = lua_chassis_log(L);
	lua_remove(L, 1);
	g_assert(n == lua_gettop(L));
	return retval;
}
static int lua_g_mem_profile(lua_State G_GNUC_UNUSED *L) {
	g_mem_profile();
	return 0;
}
/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (c) 2008 MySQL AB, 2008 Sun Microsystems, Inc.");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "export chassis-functions as chassis.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaChassis 0.2");
	lua_settable (L, -3);
}

#define CHASSIS_LUA_LOG_FUNC(level) {#level, lua_chassis_log_ ## level}

static const struct luaL_reg chassislib[] = {
	{"set_shutdown", lua_chassis_set_shutdown},
	{"log", lua_chassis_log},
/* we don't really want g_error being exposed, since it abort()s */
/*    CHASSIS_LUA_LOG_FUNC(error), */
    CHASSIS_LUA_LOG_FUNC(critical),
    CHASSIS_LUA_LOG_FUNC(warning),
    CHASSIS_LUA_LOG_FUNC(message),
    CHASSIS_LUA_LOG_FUNC(info),
    CHASSIS_LUA_LOG_FUNC(debug),
/* to get the stats of a plugin, exposed as a table */
    {"get_stats", lua_chassis_stats},
    {"mem_profile", lua_g_mem_profile},
	{NULL, NULL},
};

#undef CHASSIS_LUA_LOG_FUNC

/**
 * moves the global function 'print' to the 'os' table and
 * places 'lua_chassis_log_message' it its place.
 */
static void remap_print(lua_State *L) {
	int n = lua_gettop(L);

	lua_getglobal(L, "os"); /* sp = 1 */
	lua_getglobal(L, "print"); /* sp = 2 */
	lua_setfield(L, -2, "print"); /* sp = 1*/
    lua_pop(L, 1); /* table os. sp = 0*/
	
	lua_register(L, "print", lua_chassis_log_message);
	
	g_assert(n == lua_gettop(L));
}

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_chassis (lua_State *L) {
	luaL_register (L, "chassis", chassislib);
	set_info (L);
	remap_print(L);
	return 1;
}
