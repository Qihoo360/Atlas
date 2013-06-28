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
 

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <string.h>

#include <glib.h>

#ifdef HAVE_LUA_H
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#endif

#include "lua-load-factory.h"

typedef enum { 
	LOAD_STATE_PREFIX,
	LOAD_STATE_BUFFER,
	LOAD_STATE_POSTFIX,
	LOAD_STATE_END
} load_factory_state_t;

typedef enum { 
	LOAD_TYPE_FILE,
	LOAD_TYPE_BUFFER
} load_factory_type_t;

typedef struct {
	union {
		struct {
			const char *str;
		} string;
		struct {
		       const char *filename;
		       FILE *f;
		       char content[1024];
		} file;
	} data;

	const char *prefix;
	const char *postfix;

	load_factory_type_t  type;
	load_factory_state_t state;
} load_factory_t;

#ifdef HAVE_LUA_H
const char *loadstring_factory_reader(lua_State G_GNUC_UNUSED *L, void *data, size_t *size) {
	load_factory_t *factory = data;

	switch (factory->state) {
	case LOAD_STATE_PREFIX:
		*size = strlen(factory->prefix);
		factory->state = LOAD_STATE_BUFFER;
		return factory->prefix;
	case LOAD_STATE_BUFFER:
		switch (factory->type) {
		case LOAD_TYPE_BUFFER:
			*size = strlen(factory->data.string.str);
			factory->state = LOAD_STATE_POSTFIX;
			return factory->data.string.str;
		case LOAD_TYPE_FILE:
			g_assert(NULL != factory->data.file.f);
			*size = fread(factory->data.file.content, 1, sizeof(factory->data.file.content), factory->data.file.f);

			if (*size == 0) {
				/* eof */
				factory->data.file.content[0] = '\n';
				factory->data.file.content[1] = '\0';

				factory->state = LOAD_STATE_POSTFIX;
				*size = 1;
			}
			
			return factory->data.file.content;
		}
	case LOAD_STATE_POSTFIX:
		*size = strlen(factory->postfix);
		factory->state = LOAD_STATE_END;
		return factory->postfix;
	case LOAD_STATE_END:
		return NULL;
	}

	return NULL;
}

int luaL_loadstring_factory(lua_State *L, const char *s) {
	load_factory_t factory;

	factory.type = LOAD_TYPE_BUFFER;
	factory.data.string.str = s;
	factory.state = LOAD_STATE_PREFIX;
	factory.prefix = "return function()";
	factory.postfix = "end\n";

	return lua_load(L, loadstring_factory_reader, &factory, s);
}

int luaL_loadfile_factory(lua_State *L, const char *filename) {
	int ret;
	load_factory_t factory;

	factory.type = LOAD_TYPE_FILE;
	factory.data.file.filename = filename;
	factory.state = LOAD_STATE_PREFIX;
	factory.prefix = "return function()";
	factory.postfix = "end\n";

	factory.data.file.f = fopen(filename, "rb");

	ret = lua_load(L, loadstring_factory_reader, &factory, filename);

	fclose(factory.data.file.f);

	return ret;
}
#endif

