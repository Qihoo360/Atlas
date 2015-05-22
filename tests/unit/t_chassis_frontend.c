/* $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>

#include "chassis-frontend.h"

/**
 * test if we build the LUA_PATH correctly
 */
void t_chassis_frontend_set_lua_path(void) {
	char **lua_subdirs;
#ifdef _WIN32
#define BASEDIR "C:\\absdir\\"
#else
#define BASEDIR "/absdir/"
#endif

	lua_subdirs = g_new(char *, 3);
	lua_subdirs[0] = g_strdup("foo");
	lua_subdirs[1] = g_strdup("bar");
	lua_subdirs[2] = NULL;

	g_assert_cmpint(0, ==, chassis_frontend_init_lua_path(
		NULL, BASEDIR, lua_subdirs));

#ifdef _WIN32
	g_assert_cmpstr("C:\\absdir\\lib\\foo\\lua\\?.lua;C:\\absdir\\lib\\bar\\lua\\?.lua", ==, getenv("LUA_PATH"));
#else
	g_assert_cmpstr("/absdir/lib/foo/lua/?.lua;/absdir/lib/bar/lua/?.lua", ==, getenv("LUA_PATH"));
#endif

	g_strfreev(lua_subdirs);
}

/**
 * test if we build the LUA_CPATH correctly
 */
void t_chassis_frontend_set_lua_cpath(void) {
	char **lua_subdirs;
#ifdef _WIN32
#define BASEDIR "C:\\absdir\\"
#else
#define BASEDIR "/absdir/"
#endif

	lua_subdirs = g_new(char *, 3);
	lua_subdirs[0] = g_strdup("foo");
	lua_subdirs[1] = g_strdup("bar");
	lua_subdirs[2] = NULL;

	g_assert_cmpint(0, ==, chassis_frontend_init_lua_cpath(
		NULL, BASEDIR, lua_subdirs));

#ifdef _WIN32
	g_assert_cmpstr("C:\\absdir\\bin\\lua-?.dll", ==, getenv("LUA_CPATH"));
#else
	g_assert_cmpstr("/absdir/lib/foo/lua/?."G_MODULE_SUFFIX";/absdir/lib/bar/lua/?."G_MODULE_SUFFIX, ==, getenv("LUA_CPATH"));
#endif

	g_strfreev(lua_subdirs);
}


int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/chassis/frontend/set_lua_path", t_chassis_frontend_set_lua_path);
	g_test_add_func("/chassis/frontend/set_lua_cpath", t_chassis_frontend_set_lua_cpath);

	return g_test_run();
}

