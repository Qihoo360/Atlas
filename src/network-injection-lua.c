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
 

#include <string.h>

#include "network-injection-lua.h"

#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "glib-ext.h"
#include "glib-ext-ref.h"
#include "lua-env.h"
#include "lua-scope.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define TIME_DIFF_US(t2, t1) \
((t2.tv_sec - t1.tv_sec) * 1000000.0 + (t2.tv_usec - t1.tv_usec))


static int proxy_resultset_lua_push_ref(lua_State *L, GRef *ref);

typedef enum {
	PROXY_QUEUE_ADD_PREPEND,
	PROXY_QUEUE_ADD_APPEND
} proxy_queue_add_t;

/**
 * handle _append() and _prepend() 
 *
 * _append() and _prepend() have the same behaviour, parameters, ... 
 * just different in position
 */
#ifdef WIN32
#pragma warning (push)
#pragma warning (disable : 4715) /* don't warn about the unreached assert at the end of this function */
#endif
static int proxy_queue_add(lua_State *L, proxy_queue_add_t type) {
	GQueue *q = *(GQueue **)luaL_checkself(L);
	int resp_type = luaL_checkinteger(L, 2);
	size_t str_len;
	const char *str = luaL_checklstring(L, 3, &str_len);
	injection *inj;

	GString *query = g_string_sized_new(str_len);
	g_string_append_len(query, str, str_len);

	inj = injection_new(resp_type, query);
	inj->resultset_is_needed = FALSE;

	/* check the 4th (last) param */
	switch (luaL_opt(L, lua_istable, 4, -1)) {
	case -1:
		/* none or nil */
		break;
	case 1:
		lua_getfield(L, 4, "resultset_is_needed");
		if (lua_isnil(L, -1)) {
			/* no defined */
		} else if (lua_isboolean(L, -1)) {
			inj->resultset_is_needed = lua_toboolean(L, -1);
		} else {
			switch (type) {
			case PROXY_QUEUE_ADD_APPEND:
				return luaL_argerror(L, 4, ":append(..., { resultset_is_needed = boolean } ), is %s");
			case PROXY_QUEUE_ADD_PREPEND:
				return luaL_argerror(L, 4, ":prepend(..., { resultset_is_needed = boolean } ), is %s");
			}
		}

		lua_pop(L, 1);
		break;
	default:
		proxy_lua_dumpstack_verbose(L);
		luaL_typerror(L, 4, "table");
		break;
	}

	switch (type) {
	case PROXY_QUEUE_ADD_APPEND:
		network_injection_queue_append(q, inj);
		return 0;
	case PROXY_QUEUE_ADD_PREPEND:
		network_injection_queue_prepend(q, inj);
		return 0;
	}

	g_assert_not_reached();
}
#ifdef WIN32
#pragma warning (pop) /* restore the pragma state from before this function */
#endif

/**
 * proxy.queries:append(id, packet[, { options }])
 *
 *   id:      opaque numeric id (numeric)
 *   packet:  mysql packet to append (string)  FIXME: support table for multiple packets
 *   options: table of options (table)
 *     backend_ndx:  backend_ndx to send it to (numeric)
 *     resultset_is_needed: expose the result-set into lua (bool)
 */
static int proxy_queue_append(lua_State *L) {
	return proxy_queue_add(L, PROXY_QUEUE_ADD_APPEND);
}

static int proxy_queue_prepend(lua_State *L) {
	return proxy_queue_add(L, PROXY_QUEUE_ADD_PREPEND);
}

static int proxy_queue_reset(lua_State *L) {
	/* we expect 2 parameters */
	GQueue *q = *(GQueue **)luaL_checkself(L);

	network_injection_queue_reset(q);
    
	return 0;
}

static int proxy_queue_len(lua_State *L) {
	/* we expect 2 parameters */
	GQueue *q = *(GQueue **)luaL_checkself(L);
    
	lua_pushinteger(L, q->length);
    
	return 1;
}

static const struct luaL_reg methods_proxy_queue[] = {
	{ "prepend", proxy_queue_prepend },
	{ "append", proxy_queue_append },
	{ "reset", proxy_queue_reset },
	{ "__len", proxy_queue_len },
	{ NULL, NULL },
};

/**
 * Push a metatable onto the Lua stack containing methods to 
 * handle the query injection queue.
 */
void proxy_getqueuemetatable(lua_State *L) {
    proxy_getmetatable(L, methods_proxy_queue);
}

/**
 * Free a resultset struct when the corresponding Lua userdata is garbage collected.
 */
static int proxy_resultset_gc(lua_State *L) {
	GRef *ref = *(GRef **)luaL_checkself(L);

	g_ref_unref(ref);
    
	return 0;
}

static int proxy_resultset_fields_len(lua_State *L) {
	GRef *ref = *(GRef **)luaL_checkself(L);
	proxy_resultset_t *res = ref->udata;
	GPtrArray *fields = res->fields;
    lua_pushinteger(L, fields->len);
    return 1;
}

static int proxy_resultset_field_get(lua_State *L) {
	MYSQL_FIELD *field = *(MYSQL_FIELD **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);
        
	if (strleq(key, keysize, C("type"))) {
		lua_pushinteger(L, field->type);
	} else if (strleq(key, keysize, C("name"))) {
		lua_pushstring(L, field->name);
	} else if (strleq(key, keysize, C("org_name"))) {
		lua_pushstring(L, field->org_name);
	} else if (strleq(key, keysize, C("org_table"))) {
		lua_pushstring(L, field->org_table);
	} else if (strleq(key, keysize, C("table"))) {
		lua_pushstring(L, field->table);
	} else {
		lua_pushnil(L);
	}
    
	return 1;
}

static const struct luaL_reg methods_proxy_resultset_fields_field[] = {
	{ "__index", proxy_resultset_field_get },
	{ NULL, NULL },
};

/**
 * get a field from the result-set
 *
 */
static int proxy_resultset_fields_get(lua_State *L) {
	GRef *ref = *(GRef **)luaL_checkself(L);
	proxy_resultset_t *res = ref->udata;
	GPtrArray *fields = res->fields;
	MYSQL_FIELD *field;
	MYSQL_FIELD **field_p;
	lua_Integer ndx = luaL_checkinteger(L, 2);

	/* protect the compare */
	if (fields->len > G_MAXINT) {
		return 0;
	}
    
	if (ndx < 1 || ndx > (lua_Integer)fields->len) {
		lua_pushnil(L);
        
		return 1;
	}
    
	field = fields->pdata[ndx - 1]; /** lua starts at 1, C at 0 */
    
	field_p = lua_newuserdata(L, sizeof(field));
	*field_p = field;
    
	proxy_getmetatable(L, methods_proxy_resultset_fields_field);
	lua_setmetatable(L, -2);
    
	return 1;
}

/**
 * get the next row from the resultset
 *
 * returns a lua-table with the fields (starting at 1)
 *
 * @return 0 on error, 1 on success
 *
 */
static int proxy_resultset_rows_iter(lua_State *L) {
	GRef *ref = *(GRef **)lua_touserdata(L, lua_upvalueindex(1));
	proxy_resultset_t *res = ref->udata;
	network_packet packet;
	GPtrArray *fields = res->fields;
	gsize i;
	int err = 0;
	network_mysqld_lenenc_type lenenc_type;
    
	GList *chunk = res->row;
    
	g_return_val_if_fail(chunk != NULL, 0);

	packet.data = chunk->data;
	packet.offset = 0;

	err = err || network_mysqld_proto_skip_network_header(&packet);
	err = err || network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);
	g_return_val_if_fail(err == 0, 0); /* protocol error */
    
	switch (lenenc_type) {
	case NETWORK_MYSQLD_LENENC_TYPE_ERR:
		/* a ERR packet instead of real rows
		 *
		 * like "explain select fld3 from t2 ignore index (fld3,not_existing)"
		 *
		 * see mysql-test/t/select.test
		 */
	case NETWORK_MYSQLD_LENENC_TYPE_EOF:
		/* if we find the 2nd EOF packet we are done */
		return 0;
	case NETWORK_MYSQLD_LENENC_TYPE_INT:
	case NETWORK_MYSQLD_LENENC_TYPE_NULL:
		break;
	}
    
	lua_newtable(L);
    
	for (i = 0; i < fields->len; i++) {
		guint64 field_len;
        
		err = err || network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);
		g_return_val_if_fail(err == 0, 0); /* protocol error */

		switch (lenenc_type) {
		case NETWORK_MYSQLD_LENENC_TYPE_NULL:
			network_mysqld_proto_skip(&packet, 1);
			lua_pushnil(L);
			break;
		case NETWORK_MYSQLD_LENENC_TYPE_INT:
			err = err || network_mysqld_proto_get_lenenc_int(&packet, &field_len);
			err = err || !(field_len <= packet.data->len); /* just to check that we don't overrun by the addition */
			err = err || !(packet.offset + field_len <= packet.data->len); /* check that we have enough string-bytes for the length-encoded string */
			if (err) return luaL_error(L, "%s: row-data is invalid", G_STRLOC);
            
			lua_pushlstring(L, packet.data->str + packet.offset, field_len);

			err = err || network_mysqld_proto_skip(&packet, field_len);
			break;
		default:
			/* EOF and ERR should come up here */
			err = 1;
			break;
		}

		/* lua starts its tables at 1 */
		lua_rawseti(L, -2, i + 1);
		g_return_val_if_fail(err == 0, 0); /* protocol error */
	}
    
	res->row = res->row->next;
    
	return 1;
}

/**
 * parse the result-set of the query
 *
 * @return if this is not a result-set we return -1
 */
int parse_resultset_fields(proxy_resultset_t *res) {
	GList *chunk;

	g_return_val_if_fail(res->result_queue != NULL, -1);
    
	if (res->fields) return 0;

   	/* parse the fields */
	res->fields = network_mysqld_proto_fielddefs_new();
    
	if (!res->fields) return -1;
    
	chunk = network_mysqld_proto_get_fielddefs(res->result_queue->head, res->fields);
    
	/* no result-set found */
	if (!chunk) return -1;
    
	/* skip the end-of-fields chunk */
	res->rows_chunk_head = chunk->next;
    
	return 0;
}

static int proxy_resultset_fields_gc(lua_State *L) {
	GRef *ref = *(GRef **)luaL_checkself(L);

	g_ref_unref(ref);
    
	return 0;
}

static const struct luaL_reg methods_proxy_resultset_fields[] = {
	{ "__index", proxy_resultset_fields_get },
	{ "__len", proxy_resultset_fields_len },
	{ "__gc", proxy_resultset_fields_gc },
	{ NULL, NULL },
};

static int proxy_resultset_fields_lua_push_ref(lua_State *L, GRef *ref) {
	GRef **ref_p;

	g_ref_ref(ref);
	
	ref_p = lua_newuserdata(L, sizeof(GRef *));
	*ref_p = ref;

	proxy_getmetatable(L, methods_proxy_resultset_fields);
	lua_setmetatable(L, -2);

	return 1;
}

static int proxy_resultset_get(lua_State *L) {
	GRef *ref = *(GRef **)luaL_checkself(L);
	proxy_resultset_t *res = ref->udata;
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);
    
	if (strleq(key, keysize, C("fields"))) {
		if (!res->result_queue) {
			luaL_error(L, ".resultset.fields isn't available if 'resultset_is_needed ~= true'");
		} else {
			if (0 != parse_resultset_fields(res)) {
				/* failed */
			}
		
			if (res->fields) {
				proxy_resultset_fields_lua_push_ref(L, ref);
			} else {
				lua_pushnil(L);
			}
		}
	} else if (strleq(key, keysize, C("rows"))) {
		if (!res->result_queue) {
			luaL_error(L, ".resultset.rows isn't available if 'resultset_is_needed ~= true'");
		} else if (res->qstat.binary_encoded) {
			luaL_error(L, ".resultset.rows isn't available for prepared statements");
		} else {
			parse_resultset_fields(res); /* set up the ->rows_chunk_head pointer */
		
			if (res->rows_chunk_head) {
				res->row    = res->rows_chunk_head;

				proxy_resultset_lua_push_ref(L, ref);
		    
				lua_pushcclosure(L, proxy_resultset_rows_iter, 1);
			} else {
				lua_pushnil(L);
			}
		}
	} else if (strleq(key, keysize, C("row_count"))) {
		lua_pushinteger(L, res->rows);
	} else if (strleq(key, keysize, C("bytes"))) {
		lua_pushinteger(L, res->bytes);
	} else if (strleq(key, keysize, C("raw"))) {
		if (!res->result_queue) {
			luaL_error(L, ".resultset.raw isn't available if 'resultset_is_needed ~= true'");
		} else {
			GString *s;
			s = res->result_queue->head->data;
			lua_pushlstring(L, s->str + 4, s->len - 4); /* skip the network-header */
		}
	} else if (strleq(key, keysize, C("flags"))) {
		lua_newtable(L);
		lua_pushboolean(L, (res->qstat.server_status & SERVER_STATUS_IN_TRANS) != 0);
		lua_setfield(L, -2, "in_trans");
        
		lua_pushboolean(L, (res->qstat.server_status & SERVER_STATUS_AUTOCOMMIT) != 0);
		lua_setfield(L, -2, "auto_commit");
		
		lua_pushboolean(L, (res->qstat.server_status & SERVER_QUERY_NO_GOOD_INDEX_USED) != 0);
		lua_setfield(L, -2, "no_good_index_used");
		
		lua_pushboolean(L, (res->qstat.server_status & SERVER_QUERY_NO_INDEX_USED) != 0);
		lua_setfield(L, -2, "no_index_used");
	} else if (strleq(key, keysize, C("warning_count"))) {
		lua_pushinteger(L, res->qstat.warning_count);
	} else if (strleq(key, keysize, C("affected_rows"))) {
		/**
		 * if the query had a result-set (SELECT, ...) 
		 * affected_rows and insert_id are not valid
		 */
		if (res->qstat.was_resultset) {
			lua_pushnil(L);
		} else {
			lua_pushnumber(L, res->qstat.affected_rows);
		}
	} else if (strleq(key, keysize, C("insert_id"))) {
		if (res->qstat.was_resultset) {
			lua_pushnil(L);
		} else {
			lua_pushnumber(L, res->qstat.insert_id);
		}
	} else if (strleq(key, keysize, C("query_status"))) {
		/* hmm, is there another way to figure out if this is a 'resultset' ?
		 * one that doesn't require the parse the meta-data  */

		if (res->qstat.query_status == MYSQLD_PACKET_NULL) {
			lua_pushnil(L);
		} else {
			lua_pushinteger(L, res->qstat.query_status);
		}
	} else {
		lua_pushnil(L);
	}
    
	return 1;
}

static const struct luaL_reg methods_proxy_resultset[] = {
	{ "__index", proxy_resultset_get },
	{ "__gc", proxy_resultset_gc },
	{ NULL, NULL },
};

static int proxy_resultset_lua_push_ref(lua_State *L, GRef *ref) {
	GRef **ref_p;

	g_ref_ref(ref);
	
	ref_p = lua_newuserdata(L, sizeof(GRef *));
	*ref_p = ref;

	proxy_getmetatable(L, methods_proxy_resultset);
	lua_setmetatable(L, -2);

	return 1;
}

static int proxy_resultset_lua_push(lua_State *L, proxy_resultset_t *_res) {
	GRef **ref_p;
	GRef *ref;

	ref = g_ref_new();
	g_ref_set(ref, _res, (GDestroyNotify)proxy_resultset_free);
	
	ref_p = lua_newuserdata(L, sizeof(GRef *));
	*ref_p = ref;

	proxy_getmetatable(L, methods_proxy_resultset);
	lua_setmetatable(L, -2);

	return 1;
}

static int proxy_injection_get(lua_State *L) {
	injection *inj = *(injection **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);
    
	if (strleq(key, keysize, C("type"))) {
		lua_pushinteger(L, inj->id); /** DEPRECATED: use "inj.id" instead */
	} else if (strleq(key, keysize, C("id"))) {
		lua_pushinteger(L, inj->id);
	} else if (strleq(key, keysize, C("query"))) {
		lua_pushlstring(L, inj->query->str, inj->query->len);
	} else if (strleq(key, keysize, C("query_time"))) {
		lua_pushinteger(L, chassis_calc_rel_microseconds(inj->ts_read_query, inj->ts_read_query_result_first));
	} else if (strleq(key, keysize, C("response_time"))) {
		lua_pushinteger(L, chassis_calc_rel_microseconds(inj->ts_read_query, inj->ts_read_query_result_last));
	} else if (strleq(key, keysize, C("resultset"))) {
		/* fields, rows */
		proxy_resultset_t *res;
        
		res = proxy_resultset_new();

		/* only expose the resultset if really needed 
		   FIXME: if the resultset is encoded in binary form, we can't provide it either.
		 */
		if (inj->resultset_is_needed && !inj->qstat.binary_encoded) {	
			res->result_queue = inj->result_queue;
		}
		res->qstat = inj->qstat;
		res->rows  = inj->rows;
		res->bytes = inj->bytes;
	
		proxy_resultset_lua_push(L, res);
	} else {
		g_message("%s.%d: inj[%s] ... not found", __FILE__, __LINE__, key);
        
		lua_pushnil(L);
	}
    
	return 1;
}

static const struct luaL_reg methods_proxy_injection[] = {
	{ "__index", proxy_injection_get },
	{ NULL, NULL },
};

/**
 * Push a metatable onto the Lua stack containing methods to 
 * access the resultsets.
 */
void proxy_getinjectionmetatable(lua_State *L) {
    proxy_getmetatable(L, methods_proxy_injection);
}
