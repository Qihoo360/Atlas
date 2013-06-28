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
#include <stdlib.h>
#include <fcntl.h>

#include <errno.h>

#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include "sys-pedantic.h"
#include "string-len.h"

#include <gmodule.h>

/**
 * debug plugin
 *
 * gives access to anything in the proxy that is exported into lua-land
 *
 * all commands will be executed by lua_pcall() and a result-set or error-packet
 * will be returned
 */

struct chassis_plugin_config {
	gchar *address;                   /**< listening address of the debug interface */

	network_mysqld_con *listen_con;
};

static int lua_table_key_to_mysql_field(lua_State *L, GPtrArray *fields) {
	MYSQL_FIELD *field = NULL;

	field = network_mysqld_proto_fielddef_new();
	if (lua_isstring(L, -2) && !lua_isnumber(L, -2)) {
		/* is-string is true for strings AND numbers
		 * but a tostring() is changing a number into a 
		 * string and that trashes the lua_next() call
		 */
		field->name = g_strdup(lua_tostring(L, -2));
	} else if (lua_isnumber(L, -2)) {
		field->name = g_strdup_printf("%ld", lua_tointeger(L, -2));
	} else {
		/* we don't know how to convert the key */
		field->name = g_strdup("(hmm)");
	}
	field->type = FIELD_TYPE_VAR_STRING; /* STRING matches all values */
	g_ptr_array_add(fields, field);

	return 0;
}

int plugin_debug_con_handle_stmt(chassis *chas, network_mysqld_con *con, GString *s) {
	gsize i, j;
	GPtrArray *fields;
	GPtrArray *rows;
	GPtrArray *row;

	switch(s->str[NET_HEADER_SIZE]) {
	case COM_QUERY:
		fields = NULL;
		rows = NULL;
		row = NULL;

		/* support the basic commands sent by the mysql shell */
		if (0 == g_ascii_strncasecmp(s->str + NET_HEADER_SIZE + 1, C("select @@version_comment limit 1"))) {
			MYSQL_FIELD *field;

			fields = network_mysqld_proto_fielddefs_new();

			field = network_mysqld_proto_fielddef_new();
			field->name = g_strdup("@@version_comment");
			field->type = FIELD_TYPE_VAR_STRING;
			g_ptr_array_add(fields, field);

			rows = g_ptr_array_new();
			row = g_ptr_array_new();
			g_ptr_array_add(row, g_strdup("MySQL Enterprise Agent"));
			g_ptr_array_add(rows, row);

			network_mysqld_con_send_resultset(con->client, fields, rows);
			
		} else if (0 == g_ascii_strncasecmp(s->str + NET_HEADER_SIZE + 1, C("select USER()"))) {
			MYSQL_FIELD *field;

			fields = network_mysqld_proto_fielddefs_new();
			field = network_mysqld_proto_fielddef_new();
			field->name = g_strdup("USER()");
			field->type = FIELD_TYPE_VAR_STRING;
			g_ptr_array_add(fields, field);

			rows = g_ptr_array_new();
			row = g_ptr_array_new();
			g_ptr_array_add(row, g_strdup("root"));
			g_ptr_array_add(rows, row);

			network_mysqld_con_send_resultset(con->client, fields, rows);
		} else {
			MYSQL_FIELD *field = NULL;
			lua_State *L = chas->priv->sc->L;

			if (0 == luaL_loadstring(L, s->str + NET_HEADER_SIZE + 1) &&
			    0 == lua_pcall(L, 0, 1, 0)) {
				/* let's see what is on the stack
				 * - scalars are turned into strings
				 *   return "foo" 
				 * - 1-dim tables are turned into a single-row result-set
				 *   return { foo = "bar", baz = "foz" }
				 * - 2-dim tables are turned into a multi-row result-set
				 *   return { { foo = "bar" }, { "foz" } }
				 */
				switch (lua_type(L, -1)) {
				case LUA_TTABLE:
					/* take the names from the fields */
					fields = network_mysqld_proto_fielddefs_new();

					lua_pushnil(L);
					while (lua_next(L, -2) != 0) {
						if (lua_istable(L, -1)) {
							/* 2-dim table
							 * 
							 * we only only take the keys from the first row
							 * afterwards we ignore them
							 */
					
							lua_pushnil(L);
							while (lua_next(L, -2) != 0) {
								if (!rows) {
									/* this is the 1st round, add the keys */
									lua_table_key_to_mysql_field(L, fields);
								}

								if (!row) row = g_ptr_array_new();
								if (lua_isboolean(L, -1)) {
									g_ptr_array_add(row, g_strdup(lua_toboolean(L, -1) ? "TRUE" : "FALSE"));
								} else if (lua_isnumber(L, -1)) {
									g_ptr_array_add(row, g_strdup_printf("%.0f", lua_tonumber(L, -1)));
								} else {
									g_ptr_array_add(row, g_strdup(lua_tostring(L, -1)));
								}

								lua_pop(L, 1); /* pop the value, but keep the key on the stack */
							}
					
							if (!rows) rows = g_ptr_array_new();
							g_ptr_array_add(rows, row);

							row = NULL;
						} else {
							/* 1-dim table */
							lua_table_key_to_mysql_field(L, fields);

							if (!row) row = g_ptr_array_new();
							if (lua_isboolean(L, -1)) {
								g_ptr_array_add(row, g_strdup(lua_toboolean(L, -1) ? "TRUE" : "FALSE"));
							} else if (lua_isnumber(L, -1)) {
								g_ptr_array_add(row, g_strdup_printf("%.0f", lua_tonumber(L, -1)));
							} else {
								g_ptr_array_add(row, g_strdup(lua_tostring(L, -1)));
							}
						}

						lua_pop(L, 1); /* pop the value, but keep the key on the stack */
					}

					if (row) {
						/* in 1-dim we have to append the row to the result-set,
						 * in 2-dim this is already done and row is NULL */
						if (!rows) rows = g_ptr_array_new();
						g_ptr_array_add(rows, row);
					}

					break;
				default:
					/* a scalar value */
					fields = network_mysqld_proto_fielddefs_new();
					field = network_mysqld_proto_fielddef_new();
					field->name = g_strdup("lua");
					field->type = FIELD_TYPE_VAR_STRING;
					g_ptr_array_add(fields, field);
		
					rows = g_ptr_array_new();
					row = g_ptr_array_new();
					g_ptr_array_add(row, g_strdup(lua_tostring(L, -1)));
					g_ptr_array_add(rows, row);

					break;
				}

				lua_pop(L, 1); /* get rid of the result */

				network_mysqld_con_send_resultset(con->client, fields, rows);
			}

			/* if we don't have fields for the resultset, we should have a
			 * error-msg on the stack */
			if (!fields) {
				size_t err_len = 0;
				const char *err;

				err = lua_tolstring(L, -1, &err_len);

				network_mysqld_con_send_error(con->client, err, err_len);
				
				lua_pop(L, 1);
			}
		}

		/* clean up */
		if (fields) {
			network_mysqld_proto_fielddefs_free(fields);
			fields = NULL;
		}

		if (rows) {
			for (i = 0; i < rows->len; i++) {
				row = rows->pdata[i];

				for (j = 0; j < row->len; j++) {
					g_free(row->pdata[j]);
				}

				g_ptr_array_free(row, TRUE);
			}
			g_ptr_array_free(rows, TRUE);
			rows = NULL;
		}

		break;
	case COM_QUIT:
		break;
	case COM_INIT_DB:
		network_mysqld_con_send_ok(con->client);
		break;
	default:
		network_mysqld_con_send_error(con->client, C("unknown COM_*"));
		break;
	}

	return 0;
}

NETWORK_MYSQLD_PLUGIN_PROTO(server_con_init) {
	network_mysqld_auth_challenge *auth;
	GString *packet;

	auth = network_mysqld_auth_challenge_new();
	auth->server_version_str = g_strdup("5.1.99-proxy-debug");
	auth->thread_id     = 1;
	auth->charset       = 0x08;
	auth->server_status = SERVER_STATUS_AUTOCOMMIT | 0;
	network_mysqld_auth_challenge_set_challenge(auth);

	packet = g_string_new(NULL);
	network_mysqld_proto_append_auth_challenge(packet, auth);

	network_mysqld_queue_append(con->client, con->client->send_queue, S(packet));

	g_string_free(packet, TRUE);
	network_mysqld_auth_challenge_free(auth);
	
	con->state = CON_STATE_SEND_HANDSHAKE;

	return NETWORK_SOCKET_SUCCESS;
}

NETWORK_MYSQLD_PLUGIN_PROTO(server_read_auth) {
	GString *s;
	GList *chunk;
	network_socket *recv_sock, *send_sock;
	
	recv_sock = con->client;

	chunk = recv_sock->recv_queue->chunks->tail;
	s = chunk->data;

	/* the password is fine */
	send_sock = con->client;

	network_mysqld_con_send_ok(send_sock);

	g_string_free(chunk->data, TRUE);

	g_queue_delete_link(recv_sock->recv_queue->chunks, chunk);

	con->state = CON_STATE_SEND_AUTH_RESULT;

	return NETWORK_SOCKET_SUCCESS;
}

NETWORK_MYSQLD_PLUGIN_PROTO(server_read_query) {
	GString *s;
	GList *chunk;
	network_socket *recv_sock;

	recv_sock = con->client;

	chunk = recv_sock->recv_queue->chunks->tail;
	s = chunk->data;

	plugin_debug_con_handle_stmt(chas, con, s);
		
	g_string_free(chunk->data, TRUE);

	g_queue_delete_link(recv_sock->recv_queue->chunks, chunk);

	con->state = CON_STATE_SEND_QUERY_RESULT;

	return NETWORK_SOCKET_SUCCESS;
}

static int network_mysqld_server_connection_init(network_mysqld_con *con) {
	con->plugins.con_init             = server_con_init;

	con->plugins.con_read_auth        = server_read_auth;

	con->plugins.con_read_query       = server_read_query;

	return 0;
}

static chassis_plugin_config *network_mysqld_debug_plugin_new(void) {
	chassis_plugin_config *config;

	config = g_new0(chassis_plugin_config, 1);

	return config;
}

static void network_mysqld_debug_plugin_free(chassis_plugin_config *config) {
	if (config->listen_con) {
		/* the socket will be freed by network_mysqld_free() */
	}

	if (config->address) {
		g_free(config->address);
	}

	g_free(config);
}

/**
 * add the proxy specific options to the cmdline interface 
 */
static GOptionEntry * network_mysqld_debug_plugin_get_options(chassis_plugin_config *config) {
	guint i;

	static GOptionEntry config_entries[] = 
	{
		{ "debug-address",            0, 0, G_OPTION_ARG_STRING, NULL, "listening address:port of the debug-server (default: :4043)", "<host:port>" },
		
		{ NULL,                       0, 0, G_OPTION_ARG_NONE,   NULL, NULL, NULL }
	};

	i = 0;
	config_entries[i++].arg_data = &(config->address);

	return config_entries;
}

/**
 * init the plugin with the parsed config
 */
static int network_mysqld_debug_plugin_apply_config(chassis *chas, chassis_plugin_config *config) {
	network_mysqld_con *con;
	network_socket *listen_sock;

	if (!config->address) config->address = g_strdup(":4043");

	/** 
	 * create a connection handle for the listen socket 
	 */
	con = network_mysqld_con_new();
	network_mysqld_add_connection(chas, con);
	con->config = config;

	config->listen_con = con;
	
	listen_sock = network_socket_new();
	con->server = listen_sock;

	/* set the plugin hooks as we want to apply them to the new connections too later */
	network_mysqld_server_connection_init(con);

	/* FIXME: network_socket_set_address() */
	if (0 != network_address_set_address(listen_sock->dst, config->address)) {
		return -1;
	}

	/* FIXME: network_socket_bind() */
	if (0 != network_socket_bind(listen_sock)) {
		return -1;
	}

	/**
	 * call network_mysqld_con_accept() with this connection when we are done
	 */
	event_set(&(listen_sock->event), listen_sock->fd, EV_READ|EV_PERSIST, network_mysqld_con_accept, con);
	event_base_set(chas->event_base, &(listen_sock->event));
	event_add(&(listen_sock->event), NULL);

	return 0;
}

G_MODULE_EXPORT int plugin_init(chassis_plugin *p) {
	p->magic        = CHASSIS_PLUGIN_MAGIC;
	p->name         = g_strdup("debug");
	p->version		= g_strdup(PACKAGE_VERSION);

	p->init         = network_mysqld_debug_plugin_new;
	p->get_options  = network_mysqld_debug_plugin_get_options;
	p->apply_config = network_mysqld_debug_plugin_apply_config;
	p->destroy      = network_mysqld_debug_plugin_free;

	return 0;
}


