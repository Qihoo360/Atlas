/* $%BEGINLICENSE%$
 Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.

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
 

/**
 * expose the chassis functions into the lua space
 */


#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "network_mysqld_type.h"
#include "network-mysqld-masterinfo.h"
#include "glib-ext.h"
#include "lua-env.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define LUA_IMPORT_INT(x, y) \
	lua_getfield_literal(L, -1, C(G_STRINGIFY(y))); \
	if (!lua_isnil(L, -1)) { \
		x->y = lua_tointeger(L, -1); \
	} \
	lua_pop(L, 1);

#define LUA_IMPORT_STR(x, y) \
	lua_getfield_literal(L, -1, C(G_STRINGIFY(y))); \
	if (!lua_isnil(L, -1)) { \
		size_t s_len; \
		const char *s = lua_tolstring(L, -1, &s_len); \
		g_string_assign_len(x->y, s, s_len); \
	} \
	lua_pop(L, 1);

#define LUA_EXPORT_INT(x, y) \
	lua_pushinteger(L, x->y); \
	lua_setfield(L, -2, G_STRINGIFY(y)); 

#define LUA_EXPORT_BOOL(x, y) \
	lua_pushboolean(L, x->y); \
	lua_setfield(L, -2, G_STRINGIFY(y)); 

#define LUA_EXPORT_STR(x, y) \
	if (x->y->len) { \
		lua_pushlstring(L, S(x->y)); \
		lua_setfield(L, -2, G_STRINGIFY(y)); \
	}

static int lua_proto_get_err_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_err_packet_t *err_packet;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	err_packet = network_mysqld_err_packet_new();

	err = err || network_mysqld_proto_get_err_packet(&packet, err_packet);
	if (err) {
		network_mysqld_err_packet_free(err_packet);

		luaL_error(L, "%s: network_mysqld_proto_get_err_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);

	LUA_EXPORT_STR(err_packet, errmsg);
	LUA_EXPORT_STR(err_packet, sqlstate);
	LUA_EXPORT_INT(err_packet, errcode);

	network_mysqld_err_packet_free(err_packet);

	return 1;
}

static int lua_proto_append_err_packet (lua_State *L) {
	GString *packet;
	network_mysqld_err_packet_t *err_packet;

	luaL_checktype(L, 1, LUA_TTABLE);

	err_packet = network_mysqld_err_packet_new();

	LUA_IMPORT_STR(err_packet, errmsg);
	LUA_IMPORT_STR(err_packet, sqlstate);
	LUA_IMPORT_INT(err_packet, errcode);

	packet = g_string_new(NULL);	
	network_mysqld_proto_append_err_packet(packet, err_packet);

	network_mysqld_err_packet_free(err_packet);

	lua_pushlstring(L, S(packet));
	
	g_string_free(packet, TRUE);

	return 1;
}

static int lua_proto_get_ok_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_ok_packet_t *ok_packet;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	ok_packet = network_mysqld_ok_packet_new();

	err = err || network_mysqld_proto_get_ok_packet(&packet, ok_packet);
	if (err) {
		network_mysqld_ok_packet_free(ok_packet);

		luaL_error(L, "%s: network_mysqld_proto_get_ok_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);
	LUA_EXPORT_INT(ok_packet, server_status);
	LUA_EXPORT_INT(ok_packet, insert_id);
	LUA_EXPORT_INT(ok_packet, warnings);
	LUA_EXPORT_INT(ok_packet, affected_rows);

	network_mysqld_ok_packet_free(ok_packet);

	return 1;
}

static int lua_proto_get_masterinfo_string (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_masterinfo_t *info;

	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	info = network_mysqld_masterinfo_new();

	err = err || network_mysqld_masterinfo_get(&packet, info);
	
	if (err) {
		network_mysqld_masterinfo_free(info);
		luaL_error(L, "%s: network_mysqld_masterinfo_get() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);
        
        LUA_EXPORT_INT(info, master_lines);
	LUA_EXPORT_STR(info, master_log_file);
	LUA_EXPORT_INT(info, master_log_pos);
	LUA_EXPORT_STR(info, master_host);
	LUA_EXPORT_STR(info, master_user);
	LUA_EXPORT_STR(info, master_password);
	LUA_EXPORT_INT(info, master_port);
	LUA_EXPORT_INT(info, master_connect_retry);
	LUA_EXPORT_INT(info, master_ssl);
        LUA_EXPORT_STR(info, master_ssl_ca);
        LUA_EXPORT_STR(info, master_ssl_capath);
        LUA_EXPORT_STR(info, master_ssl_cert);
        LUA_EXPORT_STR(info, master_ssl_cipher);
        LUA_EXPORT_STR(info, master_ssl_key);
        if (info->master_lines >= 15) {
		LUA_EXPORT_INT(info, master_ssl_verify_server_cert);
	}
	
	network_mysqld_masterinfo_free(info);

	return 1;
}

static int lua_proto_append_masterinfo_string (lua_State *L) {
        GString *packet;
        network_mysqld_masterinfo_t *info;

        luaL_checktype(L, 1, LUA_TTABLE);

        info = network_mysqld_masterinfo_new();

        LUA_IMPORT_INT(info, master_lines);
        LUA_IMPORT_STR(info, master_log_file);
        LUA_IMPORT_INT(info, master_log_pos);
        LUA_IMPORT_STR(info, master_host);
        LUA_IMPORT_STR(info, master_user);
        LUA_IMPORT_STR(info, master_password);
        LUA_IMPORT_INT(info, master_port);
        LUA_IMPORT_INT(info, master_connect_retry);
        LUA_IMPORT_INT(info, master_ssl);
        LUA_IMPORT_STR(info, master_ssl_ca);
        LUA_IMPORT_STR(info, master_ssl_capath);
        LUA_IMPORT_STR(info, master_ssl_cert);
        LUA_IMPORT_STR(info, master_ssl_cipher);
        LUA_IMPORT_STR(info, master_ssl_key);
        LUA_IMPORT_INT(info, master_ssl_verify_server_cert);

        packet = g_string_new(NULL);
        network_mysqld_masterinfo_append(packet, info);

        network_mysqld_masterinfo_free(info);

        lua_pushlstring(L, S(packet));

        g_string_free(packet, TRUE);

        return 1;
}


static int lua_proto_append_ok_packet (lua_State *L) {
	GString *packet;
	network_mysqld_ok_packet_t *ok_packet;

	luaL_checktype(L, 1, LUA_TTABLE);

	ok_packet = network_mysqld_ok_packet_new();

	LUA_IMPORT_INT(ok_packet, server_status);
	LUA_IMPORT_INT(ok_packet, insert_id);
	LUA_IMPORT_INT(ok_packet, warnings);
	LUA_IMPORT_INT(ok_packet, affected_rows);

	packet = g_string_new(NULL);	
	network_mysqld_proto_append_ok_packet(packet, ok_packet);

	network_mysqld_ok_packet_free(ok_packet);
	
	lua_pushlstring(L, S(packet));
	
	g_string_free(packet, TRUE);

	return 1;
}

static int lua_proto_get_eof_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_eof_packet_t *eof_packet;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	eof_packet = network_mysqld_eof_packet_new();

	err = err || network_mysqld_proto_get_eof_packet(&packet, eof_packet);
	if (err) {
		network_mysqld_eof_packet_free(eof_packet);

		luaL_error(L, "%s: network_mysqld_proto_get_eof_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);
	LUA_EXPORT_INT(eof_packet, server_status);
	LUA_EXPORT_INT(eof_packet, warnings);

	network_mysqld_eof_packet_free(eof_packet);

	return 1;
}

static int lua_proto_append_eof_packet (lua_State *L) {
	GString *packet;
	network_mysqld_eof_packet_t *eof_packet;

	luaL_checktype(L, 1, LUA_TTABLE);

	eof_packet = network_mysqld_eof_packet_new();

	LUA_IMPORT_INT(eof_packet, server_status);
	LUA_IMPORT_INT(eof_packet, warnings);

	packet = g_string_new(NULL);	
	network_mysqld_proto_append_eof_packet(packet, eof_packet);

	network_mysqld_eof_packet_free(eof_packet);
	
	lua_pushlstring(L, S(packet));
	
	g_string_free(packet, TRUE);

	return 1;
}

static int lua_proto_get_response_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_auth_response *auth_response;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	auth_response = network_mysqld_auth_response_new();

	err = err || network_mysqld_proto_get_auth_response(&packet, auth_response);
	if (err) {
		network_mysqld_auth_response_free(auth_response);

		luaL_error(L, "%s: network_mysqld_proto_get_auth_response() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);
	LUA_EXPORT_INT(auth_response, capabilities);
	LUA_EXPORT_INT(auth_response, max_packet_size);
	LUA_EXPORT_INT(auth_response, charset);

	LUA_EXPORT_STR(auth_response, username);
	LUA_EXPORT_STR(auth_response, response);
	LUA_EXPORT_STR(auth_response, database);

	network_mysqld_auth_response_free(auth_response);

	return 1;
}

static int lua_proto_append_response_packet (lua_State *L) {
	GString *packet;
	network_mysqld_auth_response *auth_response;

	luaL_checktype(L, 1, LUA_TTABLE);

	packet = g_string_new(NULL);	
	auth_response = network_mysqld_auth_response_new();

	LUA_IMPORT_INT(auth_response, capabilities);
	LUA_IMPORT_INT(auth_response, max_packet_size);
	LUA_IMPORT_INT(auth_response, charset);

	LUA_IMPORT_STR(auth_response, username);
	LUA_IMPORT_STR(auth_response, response);
	LUA_IMPORT_STR(auth_response, database);

	if (network_mysqld_proto_append_auth_response(packet, auth_response)) {
		network_mysqld_auth_response_free(auth_response);
		g_string_free(packet, TRUE);

		luaL_error(L, "to_response_packet() failed");
        g_string_free(packet, TRUE);
		return 0;
	}
	
	network_mysqld_auth_response_free(auth_response);

	lua_pushlstring(L, S(packet));
	
	g_string_free(packet, TRUE);

	return 1;
}

static int lua_proto_get_challenge_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_auth_challenge *auth_challenge;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	auth_challenge = network_mysqld_auth_challenge_new();

	err = err || network_mysqld_proto_get_auth_challenge(&packet, auth_challenge);
	if (err) {
		network_mysqld_auth_challenge_free(auth_challenge);

		luaL_error(L, "%s: network_mysqld_proto_get_auth_challenge() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);
	LUA_EXPORT_INT(auth_challenge, protocol_version);
	LUA_EXPORT_INT(auth_challenge, server_version);
	LUA_EXPORT_INT(auth_challenge, thread_id);
	LUA_EXPORT_INT(auth_challenge, capabilities);
	LUA_EXPORT_INT(auth_challenge, charset);
	LUA_EXPORT_INT(auth_challenge, server_status);

	LUA_EXPORT_STR(auth_challenge, challenge);

	network_mysqld_auth_challenge_free(auth_challenge);

	return 1;
}

static int lua_proto_append_challenge_packet (lua_State *L) {
	GString *packet;
	network_mysqld_auth_challenge *auth_challenge;

	luaL_checktype(L, 1, LUA_TTABLE);

	auth_challenge = network_mysqld_auth_challenge_new();

	LUA_IMPORT_INT(auth_challenge, protocol_version);
	LUA_IMPORT_INT(auth_challenge, server_version);
	LUA_IMPORT_INT(auth_challenge, thread_id);
	LUA_IMPORT_INT(auth_challenge, capabilities);
	LUA_IMPORT_INT(auth_challenge, charset);
	LUA_IMPORT_INT(auth_challenge, server_status);

	LUA_IMPORT_STR(auth_challenge, challenge);

	packet = g_string_new(NULL);	
	network_mysqld_proto_append_auth_challenge(packet, auth_challenge);
	
	network_mysqld_auth_challenge_free(auth_challenge);

	lua_pushlstring(L, S(packet));
	
	g_string_free(packet, TRUE);

	return 1;
}

static int lua_proto_get_stmt_prepare_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_stmt_prepare_packet_t *cmd;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	cmd = network_mysqld_stmt_prepare_packet_new();

	err = err || network_mysqld_proto_get_stmt_prepare_packet(&packet, cmd);
	if (err) {
		network_mysqld_stmt_prepare_packet_free(cmd);

		luaL_error(L, "%s: network_mysqld_proto_get_stmt_prepare_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);

	LUA_EXPORT_STR(cmd, stmt_text);

	network_mysqld_stmt_prepare_packet_free(cmd);

	return 1;
}

/**
 * transform the OK packet of a COM_STMT_PREPARE result into a table
 */
static int lua_proto_get_stmt_prepare_ok_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_stmt_prepare_ok_packet_t *cmd;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	cmd = network_mysqld_stmt_prepare_ok_packet_new();

	err = err || network_mysqld_proto_get_stmt_prepare_ok_packet(&packet, cmd);
	if (err) {
		network_mysqld_stmt_prepare_ok_packet_free(cmd);

		luaL_error(L, "%s: network_mysqld_proto_get_stmt_prepare_ok_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);

	LUA_EXPORT_INT(cmd, stmt_id);
	LUA_EXPORT_INT(cmd, num_columns);
	LUA_EXPORT_INT(cmd, num_params);
	LUA_EXPORT_INT(cmd, warnings);

	network_mysqld_stmt_prepare_ok_packet_free(cmd);

	return 1;
}

/**
 * get the stmt-id from the com-stmt-execute packet 
 */
static int lua_proto_get_stmt_execute_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	int param_count = luaL_checkint(L, 2);
	network_mysqld_stmt_execute_packet_t *cmd;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	cmd = network_mysqld_stmt_execute_packet_new();

	err = err || network_mysqld_proto_get_stmt_execute_packet(&packet, cmd, param_count);
	if (err) {
		network_mysqld_stmt_execute_packet_free(cmd);

		luaL_error(L, "%s: network_mysqld_proto_get_stmt_execute_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);

	LUA_EXPORT_INT(cmd, stmt_id);
	LUA_EXPORT_INT(cmd, flags);
	LUA_EXPORT_INT(cmd, iteration_count);
	LUA_EXPORT_BOOL(cmd, new_params_bound);

	if (cmd->new_params_bound) {
		guint i;

		lua_newtable(L);
		for (i = 0; i < cmd->params->len; i++) {
			network_mysqld_type_t *param = g_ptr_array_index(cmd->params, i);

			lua_newtable(L);
			lua_pushnumber(L, param->type);
			lua_setfield(L, -2, "type");

			if (param->is_null) {
				lua_pushnil(L);
			} else {
				const char *const_s;
				char *_s;
				gsize s_len;
				guint64 _i;
				gboolean is_unsigned;
				double d;

				switch (param->type) {
				case MYSQL_TYPE_BLOB:
				case MYSQL_TYPE_MEDIUM_BLOB:
				case MYSQL_TYPE_LONG_BLOB:
				case MYSQL_TYPE_STRING:
				case MYSQL_TYPE_VARCHAR:
				case MYSQL_TYPE_VAR_STRING:
					if (0 != network_mysqld_type_get_string_const(param, &const_s, &s_len)) {
						return luaL_error(L, "%s: _get_string_const() failed for type = %d",
								G_STRLOC,
								param->type);
					}

					lua_pushlstring(L, const_s, s_len);
					break;
				case MYSQL_TYPE_TINY:
				case MYSQL_TYPE_SHORT:
				case MYSQL_TYPE_LONG:
				case MYSQL_TYPE_LONGLONG:
					if (0 != network_mysqld_type_get_int(param, &_i, &is_unsigned)) {
						return luaL_error(L, "%s: _get_int() failed for type = %d",
								G_STRLOC,
								param->type);
					}

					lua_pushinteger(L, _i);
					break;
				case MYSQL_TYPE_DOUBLE:
				case MYSQL_TYPE_FLOAT:
					if (0 != network_mysqld_type_get_double(param, &d)) {
						return luaL_error(L, "%s: _get_double() failed for type = %d",
								G_STRLOC,
								param->type);
					}

					lua_pushnumber(L, d);
					break;
				case MYSQL_TYPE_DATETIME:
				case MYSQL_TYPE_TIMESTAMP:
				case MYSQL_TYPE_DATE:
				case MYSQL_TYPE_TIME:
					_s = NULL;
					s_len = 0;

					if (0 != network_mysqld_type_get_string(param, &_s, &s_len)) {
						return luaL_error(L, "%s: _get_string() failed for type = %d",
								G_STRLOC,
								param->type);
					}

					lua_pushlstring(L, _s, s_len);

					if (NULL != _s) g_free(_s);
					break;
				default:
					luaL_error(L, "%s: can't decode type %d yet",
							G_STRLOC,
							param->type); /* we don't have that value yet */
					break;
				}
			}
			lua_setfield(L, -2, "value");
			lua_rawseti(L, -2, i + 1);
		}
		lua_setfield(L, -2, "params"); 
	}

	network_mysqld_stmt_execute_packet_free(cmd);

	return 1;
}

static int lua_proto_get_stmt_execute_packet_stmt_id (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_packet packet;
	GString s;
	int err = 0;
	guint32 stmt_id;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	err = err || network_mysqld_proto_get_stmt_execute_packet_stmt_id(&packet, &stmt_id);
	if (err) {
		luaL_error(L, "%s: network_mysqld_proto_get_stmt_execute_packet_stmt_id() failed", G_STRLOC);
		return 0;
	}

	lua_pushinteger(L, stmt_id);

	return 1;
}


static int lua_proto_get_stmt_close_packet (lua_State *L) {
	size_t packet_len;
	const char *packet_str = luaL_checklstring(L, 1, &packet_len);
	network_mysqld_stmt_close_packet_t *cmd;
	network_packet packet;
	GString s;
	int err = 0;

	s.str = (char *)packet_str;
	s.len = packet_len;

	packet.data = &s;
	packet.offset = 0;

	cmd = network_mysqld_stmt_close_packet_new();

	err = err || network_mysqld_proto_get_stmt_close_packet(&packet, cmd);
	if (err) {
		network_mysqld_stmt_close_packet_free(cmd);

		luaL_error(L, "%s: network_mysqld_proto_get_stmt_close_packet() failed", G_STRLOC);
		return 0;
	}

	lua_newtable(L);

	LUA_EXPORT_INT(cmd, stmt_id);

	network_mysqld_stmt_close_packet_free(cmd);

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
	lua_pushliteral (L, "export mysql protocol encoders and decoders mysql.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaMySQLProto 0.1");
	lua_settable (L, -3);
}


static const struct luaL_reg mysql_protolib[] = {
	{"from_err_packet", lua_proto_get_err_packet},
	{"to_err_packet", lua_proto_append_err_packet},
	{"from_ok_packet", lua_proto_get_ok_packet},
	{"to_ok_packet", lua_proto_append_ok_packet},
	{"from_eof_packet", lua_proto_get_eof_packet},
	{"to_eof_packet", lua_proto_append_eof_packet},
	{"from_challenge_packet", lua_proto_get_challenge_packet},
	{"to_challenge_packet", lua_proto_append_challenge_packet},
	{"from_response_packet", lua_proto_get_response_packet},
	{"to_response_packet", lua_proto_append_response_packet},
	{"from_masterinfo_string", lua_proto_get_masterinfo_string},
        {"to_masterinfo_string", lua_proto_append_masterinfo_string},
	{"from_stmt_prepare_packet", lua_proto_get_stmt_prepare_packet},
	{"from_stmt_prepare_ok_packet", lua_proto_get_stmt_prepare_ok_packet},
	{"from_stmt_execute_packet", lua_proto_get_stmt_execute_packet},
	{"stmt_id_from_stmt_execute_packet", lua_proto_get_stmt_execute_packet_stmt_id},
	{"from_stmt_close_packet", lua_proto_get_stmt_close_packet},
	{NULL, NULL},
};

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_mysql_proto (lua_State *L) {
	luaL_register (L, "proto", mysql_protolib);
	set_info (L);
	return 1;
}
