/* $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _QUERY_HANDLING_LUA_H_
#define _QUERY_HANDLING_LUA_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/**
 * embedded lua support
 */
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "network-exports.h"
#include "network-injection.h"

NETWORK_API void proxy_getqueuemetatable(lua_State *L);
NETWORK_API void proxy_getinjectionmetatable(lua_State *L);

#endif /* _QUERY_HANDLING_H_ */
