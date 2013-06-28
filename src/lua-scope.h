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
 

#ifndef _LUA_SCOPE_H_
#define _LUA_SCOPE_H_

#include <glib.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_LUA_H
#include <lua.h>
#endif

#include "chassis-exports.h"

typedef struct {
#ifdef HAVE_LUA_H
	lua_State *L;
	int L_ref;
#endif
	GMutex *mutex;	/*remove lock*/

	int L_top;
} lua_scope;

CHASSIS_API lua_scope *lua_scope_init(void) G_GNUC_DEPRECATED;
CHASSIS_API lua_scope *lua_scope_new(void);
CHASSIS_API void lua_scope_free(lua_scope *sc);

CHASSIS_API void lua_scope_get(lua_scope *sc, const char* pos);
CHASSIS_API void lua_scope_release(lua_scope *sc, const char* pos);

#define LOCK_LUA(sc) \
	lua_scope_get(sc, G_STRLOC); 

#define UNLOCK_LUA(sc) \
	lua_scope_release(sc, G_STRLOC); 

#ifdef HAVE_LUA_H
CHASSIS_API lua_State *lua_scope_load_script(lua_scope *sc, const gchar *name);
CHASSIS_API void proxy_lua_dumpstack_verbose(lua_State *L);
#endif

#endif
