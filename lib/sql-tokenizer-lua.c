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
#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#include "sql-tokenizer.h"

static int proxy_tokenize_token_get(lua_State *L) {
	sql_token *token = *(sql_token **)luaL_checkself(L); 
	size_t keysize;
	const char *key = luaL_checklstring(L, 2, &keysize);

    /* TODO: Fix bug
     *  token->text->len may be a large number, lua_pushlstring() would be crash
     *
     */
    //if(0 == lua_checkstack(L, token->text->len))
    //return 0;

    if (strleq(key, keysize, C("text"))) {
        lua_pushlstring(L, S(token->text));
        return 1;
    } else if (strleq(key, keysize, C("token_id"))) {
        lua_pushinteger(L, token->token_id);
        return 1;
    } else if (strleq(key, keysize, C("token_name"))) {
        size_t token_name_len = 0;
        const char *token_name = sql_token_get_name(token->token_id, &token_name_len);
        lua_pushlstring(L, token_name, token_name_len);
        return 1;
    } else {
        luaL_error(L, "tokens[...] has no %s field", key);
    }

	return 0;
}

int sql_tokenizer_lua_token_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_tokenize_token_get },
		{ NULL, NULL },
	};
	return proxy_getmetatable(L, methods);
}	

/**
 * get a token from the tokens array
 *
 */
static int proxy_tokenize_get(lua_State *L) {
	GPtrArray *tokens = *(GPtrArray **)luaL_checkself(L); 
	int ndx = luaL_checkinteger(L, 2);
	sql_token *token;
	sql_token **token_p;

	if (tokens->len > G_MAXINT) {
		return 0;
	}

	/* lua uses 1 is starting index */
	if (ndx < 1 || ndx > (int)tokens->len) {
		return 0;
	}

	token = tokens->pdata[ndx - 1];
	if (NULL == token) {
		lua_pushnil(L);

		return 1;
	}

	token_p = lua_newuserdata(L, sizeof(token));                          /* (sp += 1) */
	*token_p = token;

	sql_tokenizer_lua_token_getmetatable(L);
	lua_setmetatable(L, -2);             /* tie the metatable to the udata   (sp -= 1) */

	return 1;
}

/**
 * a settor for the tokens
 *
 * only allow to unset a token in the tokens array to free its memory
 */
static int proxy_tokenize_set(lua_State *L) {
	GPtrArray *tokens = *(GPtrArray **)luaL_checkself(L); 
	int ndx = luaL_checkinteger(L, 2);
	sql_token *token;

	luaL_checktype(L, 3, LUA_TNIL); /* for now we can only use = nil */

	if (tokens->len > G_MAXINT) {
		return 0;
	}

	/* lua uses 1 is starting index */
	if (ndx < 1 || ndx > (int)tokens->len) {
		return 0;
	}

	token = tokens->pdata[ndx - 1];
	if (NULL != token) {
		sql_token_free(token);
		tokens->pdata[ndx - 1] = NULL;
	}

	return 0;
}


static int proxy_tokenize_len(lua_State *L) {
	GPtrArray *tokens = *(GPtrArray **)luaL_checkself(L); 

	lua_pushinteger(L, tokens->len);

	return 1;
}

static int proxy_tokenize_gc(lua_State *L) {
	GPtrArray *tokens = *(GPtrArray **)luaL_checkself(L); 

	sql_tokens_free(tokens);

	return 0;
}


static int sql_tokenizer_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_tokenize_get },
		{ "__newindex", proxy_tokenize_set },
		{ "__len",   proxy_tokenize_len },
		{ "__gc",   proxy_tokenize_gc },
		{ NULL, NULL },
	};
	return proxy_getmetatable(L, methods);
}	

/**
 * split the SQL query into a stream of tokens
 */
int proxy_tokenize(lua_State *L) {
	size_t str_len;
	const char *str = luaL_checklstring(L, 1, &str_len);
	GPtrArray *tokens = sql_tokens_new();
	GPtrArray **tokens_p;

	sql_tokenizer(tokens, str, str_len);

	tokens_p = lua_newuserdata(L, sizeof(tokens));                          /* (sp += 1) */
	*tokens_p = tokens;

	sql_tokenizer_lua_getmetatable(L);
	lua_setmetatable(L, -2);          /* tie the metatable to the udata   (sp -= 1) */

	return 1;
}

/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (C) 2008 MySQL AB, 2008 Sun Microsystems, Inc");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "a simple tokenizer for mysql.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaMySQLTokenizer 0.1");
	lua_settable (L, -3);
}


static const struct luaL_reg mysql_tokenizerlib[] = {
	{"tokenize", proxy_tokenize},
	{NULL, NULL},
};

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_mysql_tokenizer (lua_State *L) {
	luaL_register (L, "tokenizer", mysql_tokenizerlib);
	set_info (L);
	return 1;
}

