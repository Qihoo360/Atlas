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
#include <lua.h>
#include <strings.h>
#include "lua-env.h"
#include "glib-ext.h"
#include "glib.h"
#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define ADD_PWD		1
#define ADD_ENPWD	2
#define REMOVE_PWD	3

#include "network-backend.h"
//#include "network-backend-new.h"
#include "network-mysqld.h"
#include "network-conn-pool-lua.h"
#include "network-backend-lua.h"
#include "network-address-lua.h"
#include "network-mysqld-lua.h"

static int proxy_statistics_get(lua_State *L){
    statistics_t *t = *(statistics_t **)luaL_checkself(L); 
    g_mutex_lock(&t->mutex); 
    gsize keysize = 0; 
    const char *key = luaL_checklstring(L, 2, &keysize);
    if(strleq(key, keysize, C("comselect"))){
        lua_pushinteger(L, t->com_select);
    }else if(strleq(key, keysize, C("cominsert"))){
        lua_pushinteger(L, t->com_insert);
    }else if(strleq(key, keysize, C("comreplace"))){
        lua_pushinteger(L, t->com_replace);
    }else if(strleq(key, keysize, C("comdelete"))){
        lua_pushinteger(L, t->com_delete); 
    }else if(strleq(key, keysize, C("comupdate"))){
        lua_pushinteger(L, t->com_update);
    }else if(strleq(key, keysize, C("comerror"))){
        lua_pushinteger(L, t->com_error);
    }else if(strleq(key, keysize, C("threadconnected"))){
        lua_pushinteger(L, t->threads_connected);
    }else if(strleq(key , keysize, C("threadrunning"))){
        lua_pushinteger(L, t->threads_running);
    }else if(strleq(key, keysize, C("uptime"))){
        lua_pushinteger(L, (chassis_get_rel_microseconds() - t->start_time)/(1000*1000));
    }else if(strleq(key, keysize, C("flushtime"))){
        lua_pushinteger(L,  (chassis_get_rel_microseconds() - t->up_time)/(1000*1000));
    }
    g_mutex_unlock(&t->mutex);
    return 1;
}
static int proxy_statistics_set(lua_State *L) {
    statistics_t *t = *(statistics_t **)luaL_checkself(L); 
    g_mutex_lock(&t->mutex);

    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);
    if(strleq(key, keysize, C("comselect"))){
        t->com_select = lua_tointeger(L, -1);
    }else if(strleq(key, keysize, C("cominsert"))){
        t->com_insert = lua_tointeger(L, -1);
    }else if(strleq(key, keysize, C("comreplace"))){
        t->com_replace = lua_tointeger(L, -1); 
    }else if(strleq(key, keysize, C("comdelete"))){
        t->com_delete = lua_tointeger(L, -1);
    }else if(strleq(key, keysize, C("comupdate"))){
        t->com_update = lua_tointeger(L, -1);
    }else if(strleq(key, keysize, C("comerror"))){
        t->com_error = lua_tointeger(L, -1);
    }else if(strleq(key, keysize, C("threadconnected"))){
        t->threads_connected = lua_tointeger(L, -1);
    }else if(strleq(key, keysize, C("threadrunning"))){
        t->threads_running = lua_tointeger(L, -1);
    }
    t->up_time = chassis_get_rel_microseconds();
    g_mutex_unlock(&t->mutex);
    return 1;
}
int network_statistics_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_statistics_get },
        { "__newindex", proxy_statistics_set },
        { NULL, NULL },
    };
    return proxy_getmetatable(L, methods);
}




GString *group_backend_info(network_group_backend_t *gp_backend) {
    GString *ret = NULL;
    if (NULL == gp_backend || NULL == gp_backend->group_name) {
        return ret;
    }
    ret = g_string_new(gp_backend->group_name);     
    ret = g_string_append(ret, "::");
    if (gp_backend->tag_name) {
        ret = g_string_append(ret, gp_backend->tag_name); 
    } else {
        ret = g_string_append(ret, "master");
    }  
    ret = g_string_append(ret, "::");
    ret = g_string_append(ret, gp_backend->b->raw_addr);
    if (gp_backend->type == BACKEND_TYPE_RO) {
        g_string_append_printf(ret, "@%d", gp_backend->weight);
    }//only slave backend has weight
    return ret; 
}
/**
 * get the info about a backend
 *
 * proxy.backend[0].
 *   connected_clients => clients using this backend
 *   address           => ip:port or unix-path of to the backend
 *   state             => int(BACKEND_STATE_UP|BACKEND_STATE_DOWN) 
 { "__newindex", proxy_statistics_set},
 { NULL, NULL },
 };
 return proxy_getmetatable(L, methods);
 }
**
 * get the info about a backend
 *
 * proxy.backend[0].
 *   connected_clients => clients using this backend
 *   address           => ip:port or unix-path of to the backend
 *   state             => int(BACKEND_STATE_UP|BACKEND_STATE_DOWN) 
 *   type              => int(BACKEND_TYPE_RW|BACKEND_TYPE_RO) 
 *
 * @return nil or requested information
 * @see backend_state_t backend_type_t
 */

static int proxy_backend_get(lua_State *L) {
    network_group_backend_t *gp_backend = *(network_group_backend_t **)luaL_checkself(L);
    if (NULL == gp_backend || NULL == gp_backend->b){
        return 1; 
    }
    network_backend_t *backend = gp_backend->b;
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);
    
    if (strleq(key, keysize, C("connected_clients"))) {
        lua_pushinteger(L, backend->connected_clients);
    } else if (strleq(key, keysize, C("info"))) {
        GString *ret = group_backend_info(gp_backend);  
        if (NULL != ret) {
            lua_pushlstring(L, S(ret));
            g_string_free(ret, TRUE);
        } else {
            lua_pushnil(L);
        }
    } else if (strleq(key, keysize, C("dst"))){
        network_address_lua_push(L, backend->addr);
    } else if (strleq(key, keysize, C("state"))) {
        lua_pushinteger(L, backend->state);
    } else if (strleq(key, keysize, C("type"))) {
        lua_pushinteger(L, gp_backend->type);
    } else if (strleq(key, keysize, C("uuid"))) {
        if (backend->uuid->len) {
            lua_pushlstring(L, S(backend->uuid));
        } else {
            lua_pushnil(L);
        }
    } else if (strleq(key, keysize, C("weight"))) {
        lua_pushinteger(L, gp_backend->weight);
    } else {
        lua_pushnil(L);
    }

    return 1;
}

static int proxy_backend_set(lua_State *L) {
    network_group_backend_t *gp_backend = *(network_group_backend_t **)luaL_checkself(L);
    if (NULL == gp_backend || NULL == gp_backend->b){
        return 1; 
    }
    network_backend_t *backend = gp_backend->b;
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("state"))) {
        backend->state = lua_tointeger(L, -1);
    } else if (strleq(key, keysize, C("uuid"))) {
        if (lua_isstring(L, -1)) {
            size_t s_len = 0;
            const char *s = lua_tolstring(L, -1, &s_len);

            g_string_assign_len(backend->uuid, s, s_len);
        } else if (lua_isnil(L, -1)) {
            g_string_truncate(backend->uuid, 0);
        } else {
            return luaL_error(L, "proxy.global.backends[...].%s has to be a string", key);
        }
    } else {
        return luaL_error(L, "proxy.global.backends[...].%s is not writable", key);
    }
    return 1;
}

int network_backend_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_backend_get },
       { "__newindex", proxy_backend_set },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}
// 
static int proxy_paras_get(lua_State *L) {
    dynamic_router_para_t *p = *(dynamic_router_para_t **)luaL_checkself(L); 
    if (NULL == p) {
        return 1;
    } 
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);
    if (strleq(key, keysize, C("auto_sql_filter"))) {
        lua_pushboolean(L, p->auto_sql_filter);
    } else if (strleq(key, keysize, C("sql_safe_update"))) {
        lua_pushboolean(L, p->sql_safe_update);
    } else if (strleq(key, keysize, C("set_router_rule"))) {
        lua_pushboolean(L, p->set_router_rule);
    } else if (strleq(key, keysize, C("max_conn_in_pool"))) {
        lua_pushinteger(L, p->max_conn_in_pool); 
    } else{
        return luaL_error(L, "error router paras", key);
    }
    return 1;    
}

void set_max_conn(dynamic_router_para_t *p, guint maxConn) {
    if (NULL == p) {
        return;
    }
    p->max_conn_in_pool = maxConn;
} 
void set_sql_safe_update(dynamic_router_para_t *p, gboolean v) {
    if (NULL == p) {
        return;
    }
    p->sql_safe_update = v;
}
void set_auto_sql_filter(dynamic_router_para_t *p, gboolean v) {
    if (NULL == p) {
        return;
    } 
    p->auto_sql_filter = v;
}
void set_router_rule(dynamic_router_para_t *p, gboolean v) {
    if (NULL == p) {
        return;
    }
    p->set_router_rule = v;
} 

gboolean get_bool_value(lua_State *L) {
    gboolean ret = FALSE;
    if (NULL == L) {
        return FALSE;
    }
    gchar *address = g_strdup(lua_tostring(L, -1));
    if (0 == strcasecmp(address, "false")) {
        ret = FALSE; 
    } else if (0 == strcasecmp(address, "true")) {
        ret = TRUE; 
    }    
    g_free(address);
    return ret;
} 
static int proxy_paras_set(lua_State *L) {
    dynamic_router_para_t *p = *(dynamic_router_para_t **)luaL_checkself(L);    
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);
    if (strleq(key, keysize, C("maxConn"))) {
        set_max_conn(p, lua_tointeger(L, -1)); 
    } else {
        gboolean v = get_bool_value(L); 
        if ((strleq(key, keysize, C("sqlSafeUpdate")))) {
            set_sql_safe_update(p, v);
        } else if (strleq(key, keysize, C("autoSqlFilter"))) {
            set_auto_sql_filter(p, v);
        } else if (strleq(key, keysize, C("setRouterRule"))) {
            set_router_rule(p, v);   
        } else {
            return luaL_error(L, "proxy.global.router_paras .%s is not writable", key);
        }
    } 
    return 1; 
}
/**
 * get proxy.global.backends[ndx]
 *
 * get the backend from the array of mysql backends.
 *
 * @return nil or the backend
 * @see proxy_backend_get
 */
static int proxy_backends_get(lua_State *L) {
    network_group_backend_t *backend; 
    network_group_backend_t **backend_p;

    network_nodes_t *bs = *(network_nodes_t **)luaL_checkself(L);
    int backend_ndx = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */

    /* check that we are in range for a _int_ */
    if (NULL == (backend = network_backends_get(bs, backend_ndx))) {
        lua_pushnil(L);

        return 1;
    }

    backend_p = lua_newuserdata(L, sizeof(backend)); /* the table underneath proxy.global.backends[ndx] */
    *backend_p = backend;

    network_backend_lua_getmetatable(L);
    lua_setmetatable(L, -2);

    return 1;
}

static int proxy_clients_get(lua_State *L) {
    GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
    int index = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
    gchar *ip = g_ptr_array_index(raw_ips, index);
    lua_pushlstring(L, ip, strlen(ip));
    return 1;
}

static int proxy_pwds_get(lua_State *L) {
    GPtrArray *raw_pwds = *(GPtrArray **)luaL_checkself(L);
    int index = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
    gchar *user_pwd = g_ptr_array_index(raw_pwds, index);
    lua_pushlstring(L, user_pwd, strlen(user_pwd));
    return 1;
}

/**
 * set proxy.global.backends.addslave
 *
 * add slave server into mysql backends
 *
 * @return nil or the backend
 */
//TODO(dengyihao):backend add 
static int proxy_backends_set(lua_State *L) {
    network_nodes_t *bs = *(network_nodes_t **)luaL_checkself(L);
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("addslave"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_add(bs, address, BACKEND_TYPE_RO);
        g_free(address);
    } else if (strleq(key, keysize, C("addmaster"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_add(bs, address, BACKEND_TYPE_RW);
        g_free(address);
    } else if (strleq(key, keysize, C("removebackend"))) {
        network_backends_remove(bs, lua_tointeger(L, -1));
    } else if (strleq(key, keysize, C("addclient"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_addclient(bs, address);
        g_free(address);
    } else if (strleq(key, keysize, C("removeclient"))) {
        gchar *address = g_strdup(lua_tostring(L, -1));
        network_backends_removeclient(bs, address);
        g_free(address);
    } else if (strleq(key, keysize, C("saveconfig"))) {
        //TODO(dengyihao): next version to add save config dynamic 
        network_backends_save(bs);
    } else {
        return luaL_error(L, "proxy.global.backends.%s is not writable", key);
    }
    return 1;
}

static int proxy_backends_len(lua_State *L) {
    network_nodes_t *bs = *(network_nodes_t **)luaL_checkself(L);
    lua_pushinteger(L, network_backends_count(bs));
    return 1;
}

static int proxy_clients_len(lua_State *L) {
    GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
    lua_pushinteger(L, raw_ips->len);
    return 1;
}

static int proxy_pwds_len(lua_State *L) {
    GPtrArray *raw_pwds = *(GPtrArray **)luaL_checkself(L);
    lua_pushinteger(L, raw_pwds->len);
    return 1;
}

static int proxy_clients_exist(lua_State *L) {
    GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
    gchar *client = (gchar *)lua_tostring(L, -1);
    guint i;
    for (i = 0; i < raw_ips->len; ++i) {
        if (strcmp(client, g_ptr_array_index(raw_ips, i)) == 0) {
            lua_pushinteger(L, 1);
            return 1;
        }
    }
    lua_pushinteger(L, 0);
    return 1;
}

static gboolean proxy_pwds_exist(network_nodes_t *bs, gchar *user) {
    GPtrArray *raw_pwds = bs->raw_pwds;

    guint i;
    for (i = 0; i < raw_pwds->len; ++i) {
        gchar *raw_pwd = g_ptr_array_index(raw_pwds, i);
        gchar *raw_pos = strchr(raw_pwd, ':');
        g_assert(raw_pos);
        *raw_pos = '\0';
        if (strcmp(user, raw_pwd) == 0) {
            *raw_pos = ':';
            return TRUE;
        }
        *raw_pos = ':';
    }

    return FALSE;
}

static int proxy_backends_pwds(lua_State *L) {
    network_nodes_t *bs = *(network_nodes_t **)luaL_checkself(L);
    guint type  = lua_tointeger(L, -1);
    gchar *pwd  = (gchar *)lua_tostring(L, -2);
    gchar *user = (gchar *)lua_tostring(L, -3);

    gboolean is_user_exist = proxy_pwds_exist(bs, user);
    int ret = -1;

    switch (type) {
        case ADD_PWD:
            if (is_user_exist) {
                ret = ERR_USER_EXIST;
            } else {
                ret = network_backends_addpwd(bs, user, pwd, FALSE);
            }
            break;

        case ADD_ENPWD:
            if (is_user_exist) {
                ret = ERR_USER_EXIST;
            } else {
                ret = network_backends_addpwd(bs, user, pwd, TRUE);
            }
            break;

        case REMOVE_PWD:
            if (!is_user_exist) {
                ret = ERR_USER_NOT_EXIST;
            } else {
                ret = network_backends_removepwd(bs, user);
            }
            break;

        default:
            g_assert_not_reached();
    }

    lua_pushinteger(L, ret);
    return 1;
}


static int proxy_forbidden_sqls_get(lua_State *L) {
    swap_buffer_t *t = *(swap_buffer_t **)luaL_checkself(L);
    int index = lua_tointeger(L, 2) - 1;
    gchar *sql = swap_buffer_get(t, index); 
    lua_pushlstring(L, sql, strlen(sql)); 
    return 1;
}
static int proxy_forbidden_sqls_set(lua_State *L) {
    swap_buffer_t *t = *(swap_buffer_t **)luaL_checkself(L); 
    gsize keysize = 0;
    const char *key = luaL_checklstring(L, 2, &keysize);

    if (strleq(key, keysize, C("remove_sql"))) {
        swap_buffer_remove(t, lua_tointeger(L, -1)); 
    } else if (strleq(key, keysize, C("add_sql"))) {
       gchar *address = g_strdup(lua_tostring(L, -1)); 
       swap_buffer_add(t, address);
       g_free(address);
    } else {
        return luaL_error(L, "%s error", key);
    }
    return 1;
}
static int proxy_forbidden_sqls_len(lua_State *L) {
    swap_buffer_t *t = *(swap_buffer_t **)luaL_checkself(L);
    lua_pushinteger(L, t->arr[*(t->idx)]->len);
    return 1;
}
int network_paras_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_paras_get },
        { "__newindex", proxy_paras_set },
        { NULL, NULL },
    };
    return proxy_getmetatable(L, methods);
}
int network_backends_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_backends_get },
        { "__newindex", proxy_backends_set },
        { "__len", proxy_backends_len },
        { "__call", proxy_backends_pwds },
        { NULL, NULL },
    };
    return proxy_getmetatable(L, methods);
}
int network_clients_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_clients_get },
        { "__len", proxy_clients_len },
        { "__call", proxy_clients_exist },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}

int network_pwds_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_pwds_get },
        { "__len", proxy_pwds_len },
        { NULL, NULL },
    };

    return proxy_getmetatable(L, methods);
}
int network_forbidden_sqls_lua_getmetatable(lua_State *L) {
    static const struct luaL_reg methods[] = {
        { "__index", proxy_forbidden_sqls_get },
        { "__newindex", proxy_forbidden_sqls_set },
        { "__len", proxy_forbidden_sqls_len },
        { NULL, NULL },
    };
    return proxy_getmetatable(L, methods);
}
