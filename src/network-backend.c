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

#include <stdlib.h> 
#include <string.h>
#include <openssl/evp.h>
#include <glib.h>
#include "lemon/sqliteInt.h"
#include "network-mysqld-packet.h"
#include "network-backend.h"
#include "chassis-plugin.h"
#include "glib-ext.h"
#include "chassis-sharding.h"
#include "util/parse_config_file.h"
#include "util/save_config_file.h"
#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

/**
 * common help function
 **/

static void swap(guint *a, guint *b) {
    guint t = *a;
    *a = *b;
    *b = t; 
}
static void random_shuffle(GPtrArray *p) {
    guint i; 
    srand(time(NULL));  
    for (i = 1; p && i < p->len; i++) {
        guint *t = g_ptr_array_index(p, i);
        guint *o = g_ptr_array_index(p, rand()%(i+1));    
        swap(t, o);
    }
}

/* group backend node sort fun c */
gint compare(gconstpointer a, gconstpointer b) {
    gint ret = 0; 
    network_group_backend_t *first = *(network_group_backend_t **)a;
    network_group_backend_t *second = *(network_group_backend_t **)b;
    if (NULL == first || NULL == second) {
        return ret; 
    }
    ret = strcmp(first->group_name, second->group_name); 
    if (0 == ret) {
        if (first->tag_name && second->tag_name) {
            ret = strcmp(first->tag_name, second->tag_name);
        }
    } 
    return ret;
}

gchar *get_raw_addr(gchar *ip, guint *weight) {
    if (NULL == ip) { 
        return NULL; 
    }
    gchar *p = NULL;
    p = strrchr(ip, '@');
    if (p) {
        *p = '\0';
        *weight = atoi(p + 1); 
    } else {
        *weight = 1; 
    } 
    gchar *t = g_strdup(ip); 
    if(p) *p = '@';
    return t;
}
static void append_idx_by_weight(GPtrArray *p, guint weight, guint idx){
    if (!p) { 
        return; 
    } 
    guint m; 
    for (m = 0; m < weight; ++m) {
        gint *i = g_new0(int, 1);
        *i = idx;
        g_ptr_array_add(p, i);
    }
}
network_backend_t *network_backend_new(gchar *raw_addr, guint event_thread_count) {
    network_backend_t *b = g_new0(network_backend_t, 1);

    b->pools = g_ptr_array_new();
    guint i;
    for (i = 0; i <= event_thread_count; ++i) {
        network_connection_pool* pool = network_connection_pool_new();
        g_ptr_array_add(b->pools, pool);
    }

    b->uuid = g_string_new(NULL);
    b->addr = network_address_new();
    b->cite_cnt = 1;
    b->raw_addr = g_strdup(raw_addr);
    if (b->addr) {
        if (0 != network_address_set_address(b->addr, raw_addr)) {
            network_backend_free(b);
            return NULL; 
        }
    }
    g_mutex_init(&b->mutex);
    return b;
}

void network_backend_free(gpointer t) {
    network_backend_t *b = t;
    if (NULL == b) return;

    guint i;
    for (i = 0; i < b->pools->len; ++i) {
        network_connection_pool* pool = g_ptr_array_index(b->pools, i);
        network_connection_pool_free(pool);
    }
    g_ptr_array_free(b->pools, TRUE);

    if (b->addr)     network_address_free(b->addr);
    if (b->uuid)     g_string_free(b->uuid, TRUE);

    g_mutex_clear(&b->mutex);
    g_free(b->raw_addr);
    g_free(b);
}
void backend_cited_inc(network_backend_t *b) {
    if (NULL == b) {
        return; 
    }
    b->cite_cnt += 1;
} 
void backend_cited_dec(network_backend_t *b) {
    if (NULL == b || b->cite_cnt <= 0) {
        return;
    } 
    b->cite_cnt -= 1;
}
gboolean backend_cited_zero(network_backend_t *b) {
    if (NULL == b || 0 == b->cite_cnt) {
        return TRUE;
    } 
    return FALSE;
}
network_backends_t *network_backends_new(guint event_thread_count, gchar *default_file) {
    network_backends_t *bs;

    bs = g_new0(network_backends_t, 1);

    bs->backends = g_ptr_array_new();
    g_mutex_init(&bs->backends_mutex);	/*remove lock*/
    bs->global_wrr = g_wrr_poll_new();
    bs->event_thread_count = event_thread_count;
    bs->default_file = g_strdup(default_file);
    bs->raw_ips = g_ptr_array_new_with_free_func(g_free);
    bs->raw_pwds = g_ptr_array_new_with_free_func(g_free);

    return bs;
}

g_wrr_poll *g_wrr_poll_new() {
    g_wrr_poll *global_wrr;

    global_wrr = g_new0(g_wrr_poll, 1);

    global_wrr->max_weight = 1;
    global_wrr->cur_weight = 0;
    global_wrr->next_ndx = 0;

    return global_wrr;
}

void g_wrr_poll_free(g_wrr_poll *global_wrr) {
    g_free(global_wrr);
}

void network_backends_free(network_backends_t *bs) {
    gsize i;

    if (!bs) return;

    g_mutex_lock(&bs->backends_mutex);	/*remove lock*/
    for (i = 0; i < bs->backends->len; i++) {
        network_backend_t *backend = bs->backends->pdata[i];

        network_backend_free(backend);
    }
    g_mutex_unlock(&bs->backends_mutex);	/*remove lock*/

    g_ptr_array_free(bs->backends, TRUE);
    g_mutex_clear(&bs->backends_mutex);	/*remove lock*/

    g_wrr_poll_free(bs->global_wrr);
    g_free(bs->default_file);

    g_ptr_array_free(bs->raw_ips, TRUE);
    g_ptr_array_free(bs->raw_pwds, TRUE);

    g_free(bs);
}

int network_backends_remove(network_nodes_t *t, guint index) {
    g_mutex_lock(&t->nodes_mutex);
    network_group_backend_t *gp_backend = g_ptr_array_remove_index(t->group_backends, index);
    if (NULL == gp_backend) {
        g_mutex_unlock(&t->nodes_mutex);
        return 0; 
    }
    //TODO(dengyihao): reasonable
    group_info_t *group = g_hash_table_lookup(t->nodes, gp_backend->group_name);    
    if (NULL == group) {
        return 0;
    }

    if (NULL != gp_backend->tag_name) {
        guint i;  
        slaves_tag_t *taged_slave = NULL;
        for (i = 0; group->slaves && i < group->slaves->len; i++) {
            taged_slave = group->slaves->pdata[i];
            if (0 == strcasecmp(taged_slave->tag_name, gp_backend->tag_name)) {
                break;
            }
        }
        g_ptr_array_remove_index(taged_slave->addrs, gp_backend->idx); 

        g_ptr_array_set_size(taged_slave->index, 0);
        network_group_backend_t *t_ = NULL;  
        for (i = 0; taged_slave->addrs && i < taged_slave->addrs->len; i++) {
            t_ = taged_slave->addrs->pdata[i]; 
            append_idx_by_weight(taged_slave->index, t_->weight, i);
        }
        random_shuffle(taged_slave->index);
    } else {
        group->master = NULL; 
    }
              
    backend_cited_dec(gp_backend->b);
    if (backend_cited_zero(gp_backend->b)) {
        g_hash_table_remove(t->conn, gp_backend->b->raw_addr);
        gp_backend->b = NULL;
    }
    
    network_group_backend_free(gp_backend);
    g_mutex_unlock(&t->nodes_mutex);

    return 1;
}

void copy_key(gpointer k, gpointer v, gpointer t) {
    guint *key = k;
    guint *value = v;
    GHashTable *table = t;

    guint *new_key = g_new0(guint, 1);
    *new_key = *key;
    g_hash_table_add(table, new_key);
}

void copy_pwd(gpointer k, gpointer v, gpointer t) {
    gchar *key = k;
    GString *value = v;
    GHashTable *table = t;
    g_hash_table_insert(table, g_strdup(key), g_string_new_len(S(value)));
}

int network_backends_addclient(network_nodes_t *bs, gchar *address) {
    guint i;
    for (i = 0; i < bs->raw_ips->len; ++i) {
        gchar *ip = g_ptr_array_index(bs->raw_ips, i);
        if (strcmp(address, ip) == 0) {	//若有相同client_ip则不允许add
            return -1;
        }
    }
    g_ptr_array_add(bs->raw_ips, g_strdup(address));

    guint* sum = g_new0(guint, 1);
    char* token;
    while ((token = strsep(&address, ".")) != NULL) {
        *sum = (*sum << 8) + atoi(token);
    }
    *sum = htonl(*sum);

    gint index = bs->ip_table_index;
    GHashTable *old_table = bs->ip_table[index];
    GHashTable *new_table = bs->ip_table[1-index];
    g_hash_table_remove_all(new_table);
    g_hash_table_foreach(old_table, copy_key, new_table);
    g_hash_table_add(new_table, sum);
    g_atomic_int_set(&bs->ip_table_index, 1-index);

    return 0;
}

static char *encrypt(char *in) {
    EVP_CIPHER_CTX ctx;
    const EVP_CIPHER *cipher = EVP_des_ecb();
    unsigned char key[] = "aCtZlHaUs";

    //1. DES加密
    EVP_CIPHER_CTX_init(&ctx);
    if (EVP_EncryptInit_ex(&ctx, cipher, NULL, key, NULL) != 1) return NULL;

    int inl = strlen(in);

    unsigned char inter[512] = {};
    int interl = 0;

    if (EVP_EncryptUpdate(&ctx, inter, &interl, in, inl) != 1) return NULL;
    int len = interl;
    if (EVP_EncryptFinal_ex(&ctx, inter+len, &interl) != 1) return NULL;
    len += interl;
    EVP_CIPHER_CTX_cleanup(&ctx);

    //2. Base64编码
    EVP_ENCODE_CTX ectx;
    EVP_EncodeInit(&ectx);

    char *out = g_malloc0(512);
    int outl = 0;

    EVP_EncodeUpdate(&ectx, out, &outl, inter, len);
    len = outl;
    EVP_EncodeFinal(&ectx, out+len, &outl);
    len += outl;

    if (out[len-1] == 10) out[len-1] = '\0';
    return out;
}

char *decrypt(char *in) {
    //1. Base64解码
    EVP_ENCODE_CTX dctx;
    EVP_DecodeInit(&dctx);

    int inl = strlen(in);
    unsigned char inter[512] = {};
    int interl = 0;

    if (EVP_DecodeUpdate(&dctx, inter, &interl, in, inl) == -1)
        return NULL;
    int len = interl;
    if (EVP_DecodeFinal(&dctx, inter+len, &interl) != 1)
        return NULL;
    len += interl;

    //2. DES解码
    EVP_CIPHER_CTX ctx;
    EVP_CIPHER_CTX_init(&ctx);
    const EVP_CIPHER *cipher = EVP_des_ecb();

    unsigned char key[] = "aCtZlHaUs";
    if (EVP_DecryptInit_ex(&ctx, cipher, NULL, key, NULL) != 1) return NULL;

    char *out = g_malloc0(512);
    int outl = 0;

    if (EVP_DecryptUpdate(&ctx, out, &outl, inter, len) != 1) {
        g_free(out);
        return NULL;
    }
    len = outl;
    if (EVP_DecryptFinal_ex(&ctx, out+len, &outl) != 1) {
        g_free(out);
        return NULL;
    }
    len += outl;

    EVP_CIPHER_CTX_cleanup(&ctx);

    out[len] = '\0';
    return out;
}
guint decrypt_new(gchar *in, gchar *out){
    //1. Base64解码
    EVP_ENCODE_CTX dctx;
    EVP_DecodeInit(&dctx);

    int inl = strlen(in);
    unsigned char inter[512] = {};
    int interl = 0;

    if (EVP_DecodeUpdate(&dctx, inter, &interl, in, inl) == -1) return 0;
    int len = interl;
    if (EVP_DecodeFinal(&dctx, inter+len, &interl) != 1) return 0;
    len += interl;

    //2. DES解码
    EVP_CIPHER_CTX ctx;
    EVP_CIPHER_CTX_init(&ctx);
    const EVP_CIPHER *cipher = EVP_des_ecb();

    unsigned char key[] = "aCtZlHaUs";
    if (EVP_DecryptInit_ex(&ctx, cipher, NULL, key, NULL) != 1) return 0;

    int outl = 0;

    if (EVP_DecryptUpdate(&ctx, out, &outl, inter, len) != 1) {
        return 0;
    }
    len = outl;
    if (EVP_DecryptFinal_ex(&ctx, out+len, &outl) != 1) {
        return 0;
    }
    len += outl;

    EVP_CIPHER_CTX_cleanup(&ctx);

    out[len] = '\0';
    return 1;
} 
int network_backends_addpwd(network_nodes_t *bs, gchar *user, gchar *pwd, gboolean is_encrypt) {
    GString *hashed_password = g_string_new(NULL);
    if (is_encrypt) {
        gchar *decrypt_pwd = decrypt(pwd);
        if (decrypt_pwd == NULL) {
            g_warning("failed to decrypt %s\n", pwd);
            g_free(hashed_password);
            return ERR_PWD_DECRYPT;
        }
        network_mysqld_proto_password_hash(hashed_password, decrypt_pwd, strlen(decrypt_pwd));
        g_free(decrypt_pwd);
        g_ptr_array_add(bs->raw_pwds, g_strdup_printf("%s:%s", user, pwd));
    } else {
        gchar *encrypt_pwd = encrypt(pwd);
        if (encrypt_pwd == NULL) {
            g_warning("failed to encrypt %s\n", pwd);
            g_free(hashed_password);
            return ERR_PWD_ENCRYPT;
        }
        g_ptr_array_add(bs->raw_pwds, g_strdup_printf("%s:%s", user, encrypt_pwd));
        g_free(encrypt_pwd);
        network_mysqld_proto_password_hash(hashed_password, pwd, strlen(pwd));
    }

    gint index = *(bs->pwd_table_index);
    GHashTable *old_table = bs->pwd_table[index];
    GHashTable *new_table = bs->pwd_table[1-index];
    g_hash_table_remove_all(new_table);
    g_hash_table_foreach(old_table, copy_pwd, new_table);
    g_hash_table_insert(new_table, g_strdup(user), hashed_password);
    g_atomic_int_set(bs->pwd_table_index, 1-index);

    return PWD_SUCCESS;
}

int network_backends_removeclient(network_nodes_t *bs, gchar *address) {
    if (!bs && !bs->raw_ips) {
        return -1;
    }

    guint i;
    for (i = 0; i < bs->raw_ips->len; ++i) {
        gchar *ip = g_ptr_array_index(bs->raw_ips, i);
        if (strcmp(address, ip) == 0) {	//找到相同client_ip才可remove
            g_ptr_array_remove_index(bs->raw_ips, i);

            guint sum;
            char* token;
            while ((token = strsep(&address, ".")) != NULL) {
                sum = (sum << 8) + atoi(token);
            }
            sum = htonl(sum);

            gint index = bs->ip_table_index;
            GHashTable *old_table = bs->ip_table[index];
            GHashTable *new_table = bs->ip_table[1-index];
            g_hash_table_remove_all(new_table);
            g_hash_table_foreach(old_table, copy_key, new_table);
            g_hash_table_remove(new_table, &sum);
            g_atomic_int_set(&bs->ip_table_index, 1-index);

            return 0;
        }
    }

    return -1;
}

int network_backends_removepwd(network_nodes_t *bs, gchar *address) {
    guint i;
    for (i = 0; i < bs->raw_pwds->len; ++i) {
        gchar *user_pwd = g_ptr_array_index(bs->raw_pwds, i);
        gchar *pos = strchr(user_pwd, ':');
        g_assert(pos);
        *pos = '\0';
        if ((strcmp(address, user_pwd) == 0)) {	//找到相同user才能remove
            *pos = ':';
            g_ptr_array_remove_index(bs->raw_pwds, i);

            gint index = *(bs->pwd_table_index);
            GHashTable *old_table = bs->pwd_table[index];
            GHashTable *new_table = bs->pwd_table[1-index];
            g_hash_table_remove_all(new_table);
            g_hash_table_foreach(old_table, copy_pwd, new_table);
            g_hash_table_remove(new_table, address);
            g_atomic_int_set(bs->pwd_table_index, 1-index);

            return PWD_SUCCESS;
        }
        *pos = ':';
    }

    return ERR_USER_NOT_EXIST;
}

void append_key(guint *key, guint *value, GString *str) {
    g_string_append_c(str, ',');
    guint sum = *key;

    g_string_append_printf(str, "%u", sum & 0x000000FF);

    guint i;
    for (i = 1; i <= 3; ++i) {
        sum >>= 8;
        g_string_append_printf(str, ".%u", sum & 0x000000FF);
    }
}

//
// 
//
gint set_json_object_string_array_backends(json_object *obj, gchar *k, GPtrArray *arr) {
    gint ret = 0; 
    if (NULL == obj || NULL == arr) {
        return ret;
    }

    json_object *obj_ = json_object_object_get(obj, k);
    if (NULL == obj_) {
        obj_ = json_object_new_array(); 
        json_object_object_add(obj, k, obj_);
    }
    ret = 1;
    gint len = json_object_array_length(obj_);
    gint i; 
    for (i  = 0 ;i < arr->len; i++) {
        network_group_backend_t *t = g_ptr_array_index(arr, i); 
        if (NULL == t) {
            continue;
        }
        gchar *ch = g_strdup_printf("%s@%d", t->b->raw_addr, t->weight); 
        json_object_array_put_idx(obj_, i, json_object_new_string(ch));
        g_free(ch);
    }

    if (i < len) {
        json_object_array_del_idx(obj_, i, len - i);
    }
    return ret;
}

gchar *get_json_object_string(json_object *json, const gchar *key) {
    gchar *ret = NULL;
    if (NULL == json || NULL == key) { 
        return ret;
    } 

    json_object *obj = json_object_object_get(json, key);
    if (NULL == obj || FALSE == json_object_is_type(obj, json_type_string)) {
        return ret; 
    } 

    ret = (gchar *)json_object_get_string(obj);
    return ret;
} 
void update_groups_member(gpointer k, gpointer v, gpointer json) {
    if (NULL == json || NULL == k || NULL == v) {
        return; 
    }

    gchar *key = (gchar *)k; 
    group_info_t *value = (group_info_t *)v;
    json_object *js = json;

    json_object *obj = json_object_object_get(js, "groups");
    if (NULL != obj) {
        gint len = json_object_array_length(obj); 
        json_object *t = NULL; 
        gint i;
        for (i = 0; i < len; i++) {
            t = json_object_array_get_idx(obj, i); 
            gchar *name = (gchar *)get_json_object_string(t, "name");      
            if (0 == strcasecmp(name, key)) { break; } 
        }
        if (i == len) { return; }

        //set master member  
        if (value->master != NULL && value->master->b != NULL) {
            set_json_object_string(t, "master", value->master->b->raw_addr);
        } else {
            set_json_object_string(t, "master", "");
        }

        //TODO(dengyiho):to make code more elegance
        // set slave member 
        json_object *slaves_obj = json_object_object_get(t, "slaves");
        gint slen = json_object_array_length(slaves_obj);
        for (i = 0; i < slen; i++) {
            json_object *obj_ = json_object_array_get_idx(slaves_obj, i); 
            gchar *name = (gchar *)get_json_object_string(obj_, "name"); 

            slaves_tag_t *taged_slave = NULL; 
            gint j;
            for(j = 0; j < value->slaves->len; j++) {
                slaves_tag_t *t_ = g_ptr_array_index(value->slaves, j);
                if (0 == strcasecmp(t_->tag_name, name)) {
                    taged_slave = t_; 
                    break;
                }
            } 
            if (NULL == taged_slave) {
                continue;
            }
            set_json_object_string_array_backends(obj_, "ips", taged_slave->addrs);
        }
    }
}

//TODO(dengyihao): change func name 
gboolean network_backends_save(network_nodes_t *n) {
    gboolean ret = FALSE;
    if (NULL == n || NULL == n->default_file) {
        return ret;
    }
    char *file = n->default_file;

    json_object *jso = open_file(file); 
    if (NULL == jso) {
        g_critical("config file(%s) is error", n->default_file);
        return ret;
    }

    /*-----------------comm para-----------------------*/ 
    set_json_object_string_array(jso, "pwds", n->raw_pwds);
    set_json_object_string_array(jso, "clients-ips", n->raw_ips);
    set_json_object_string_array(jso, "forbidden-sql",swap_buffer_in_use(n->forbidden_sql));

    if (n->router_para) {
        set_json_object_bool(jso, "sql-safe-update", n->router_para->sql_safe_update);
        set_json_object_bool(jso, "auto-sql-filter", n->router_para->auto_sql_filter);
        set_json_object_bool(jso, "set-router-rule", n->router_para->set_router_rule);
        set_json_object_int(jso, "max-conn-in-pool", n->router_para->max_conn_in_pool);
    } 

    /*-------------------group -----------------------*/ 
    g_hash_table_foreach(n->nodes, update_groups_member, jso); 
    save_config_file(jso, file);

    return ret = TRUE;
}

/*
 * FIXME: 1) remove _set_address, make this function callable with result of same
 *        2) differentiate between reasons for "we didn't add" (now -1 in all cases)
 */
//TODO(dengyihao): to add group dynamicly  
int network_backends_add(network_nodes_t *ns, /* const */ gchar *gp_addr, backend_type_t type) {
    if (NULL == ns || NULL == gp_addr) {
        return -1;
    }
    gchar **p = NULL; 
    p = g_strsplit(gp_addr, "::", 3);
    if (NULL == p || NULL == *p || NULL == *(p + 1)) {
        return -1;    
    } 

    gchar *group_name = *p;
    gchar *tag_name =  NULL;  
    gchar *addr = NULL; 

    if (*(p + 2) != NULL) {
        tag_name = *(p + 1);
        addr = *(p + 2);
    } else {
        addr = *(p + 1);  
    }

    g_mutex_lock(&ns->nodes_mutex);
    group_info_t *group = g_hash_table_lookup(ns->nodes, group_name);
    if (NULL == group) {
        g_mutex_unlock(&ns->nodes_mutex);
        g_strfreev(p);
        return -1;
    } 

    guint weight;
    gchar *db_addr = get_raw_addr(addr, &weight);
    network_backend_t *backend = g_hash_table_lookup(ns->conn, db_addr);
    if (NULL == backend) {
        backend = network_backend_new(db_addr, ns->event_thread_count); 
        if (NULL != backend) {
            g_hash_table_insert(ns->conn, g_strdup(db_addr), backend);
        } else {
            g_mutex_unlock(&ns->nodes_mutex);
            g_free(db_addr);
            g_strfreev(p);
            return -1;
        }
    } else {
        g_mutex_unlock(&ns->nodes_mutex);
        g_free(db_addr);
        g_strfreev(p);
        return -1;
    } 

    if (type == BACKEND_TYPE_RW) {
        //TODO(dengyihao): the same node is added more than once  
        network_group_backend_t *m_ = group->master;
        if (NULL != m_) {
            g_ptr_array_remove(ns->group_backends, m_);
            if (NULL != m_->b)  {
                backend_cited_dec(m_->b);  
                if (backend_cited_zero(m_->b)) {
                    if (g_hash_table_lookup(ns->conn, m_->b->raw_addr)) {
                        g_hash_table_remove(ns->conn, m_->b->raw_addr);
                    }
                }
            }
            network_group_backend_free(m_);
            group->master = NULL;
        }
        group->master = network_group_backend_new(group_name, NULL, backend, weight, BACKEND_TYPE_RW, 0);
        backend_cited_inc(backend);
        g_ptr_array_add(ns->group_backends, group->master);
    } else if (type == BACKEND_TYPE_RO) {
        guint i;
        slaves_tag_t *taged_slave = NULL;
        for (i = 0; group->slaves &&  i < group->slaves->len; i++) {
            slaves_tag_t *t_ = group->slaves->pdata[i]; 
            if (0 == strcasecmp(t_->tag_name, tag_name)) {
                taged_slave = t_;
                break;
            } 
        }
        if (NULL != taged_slave) { 
            network_group_backend_t *del = NULL;
            for(i = 0; taged_slave->addrs && i < taged_slave->addrs->len; i++) {
                network_group_backend_t *t_ = taged_slave->addrs->pdata[i];
                if (t_ && 0 == strcmp(t_->group_name, group_name) 
                        && 0 == strcmp(t_->tag_name, tag_name) 
                        && 0 == strcmp(t_->b->raw_addr, db_addr)) {
                    del = t_;
                    break;
                }
            }
            if (del != NULL) {
                g_ptr_array_remove(taged_slave->addrs, del);
                g_ptr_array_remove(ns->group_backends, del);
                network_group_backend_free(del);
            } else {
                backend_cited_inc(backend);
            }

            network_group_backend_t *m_ = network_group_backend_new(group_name, 
                    tag_name, 
                    backend, 
                    weight, 
                    BACKEND_TYPE_RO, 
                    taged_slave->addrs->len); 

            g_ptr_array_add(taged_slave->addrs, m_);
            g_ptr_array_add(ns->group_backends, m_);

            g_ptr_array_set_size(taged_slave->index, 0);
            for( i = 0; taged_slave->addrs && i < taged_slave->addrs->len; i++) {
                network_group_backend_t *t_  = taged_slave->addrs->pdata[i]; 
                append_idx_by_weight(taged_slave->index, t_->weight, i);
            }
            random_shuffle(taged_slave->index);
        }
    }

    //sort by group name and tags name  
    if (ns->group_backends) {
        g_ptr_array_sort(ns->group_backends, compare); 
    }

    g_mutex_unlock(&ns->nodes_mutex);
    g_free(db_addr);
    g_strfreev(p);
    return 0;
}

network_group_backend_t *network_backends_get(network_nodes_t *bs, guint ndx) {
    if (ndx >= network_backends_count(bs)) return NULL;

    /* FIXME: shouldn't we copy the backend or add ref-counting ? */	
    return bs->group_backends->pdata[ndx];
}

guint network_backends_count(network_nodes_t *bs) {
    guint len;

    g_mutex_lock(&bs->nodes_mutex);	/*remove lock*/
    len = bs->group_backends->len;
    g_mutex_unlock(&bs->nodes_mutex);	/*remove lock*/

    return len;
}

network_group_backend_t *network_group_backend_new(gchar *grp_name, \
        gchar *tag_name, \
        network_backend_t *b,\
        guint weight, \
        backend_type_t type, \
        guint idx) {
    if (NULL == grp_name || NULL == b) {
        return NULL;
    } 
    network_group_backend_t *t = g_new0(network_group_backend_t, 1); 
    t->b = b;
    t->type = type;
    t->weight = weight;
    t->tag_name = g_strdup(tag_name);
    t->group_name = g_strdup(grp_name);
    return t;
}

void network_group_backend_free(network_group_backend_t *t) {
    if (NULL == t) {
        return;
    }
    g_free(t->group_name);
    g_free(t->tag_name);
    g_free(t); 
} 

// slave group info new and free
slaves_tag_t *slaves_tag_new(gchar *name){
    slaves_tag_t *s = g_new0(slaves_tag_t, 1); 
    s->tag_name = g_strdup(name);
    s->addrs = g_ptr_array_new();
    s->index = g_ptr_array_new_with_free_func(g_free); 
    return s;
}

void slaves_tag_free(slaves_tag_t *taged_slaves){
    if (NULL == taged_slaves) { 
        return; 
    }

    guint i;
    for (i = 0; taged_slaves->addrs && i < taged_slaves->addrs->len; ++i) {
        network_group_backend_t *t = g_ptr_array_index(taged_slaves->addrs, i);
        network_group_backend_free(t);
    } 
    g_ptr_array_free(taged_slaves->addrs, TRUE);
    g_ptr_array_free(taged_slaves->index, TRUE);

    g_free(taged_slaves->tag_name);
    g_free(taged_slaves);
}


group_info_t *group_info_new(gchar *name) {
    if (NULL == name) {
        return NULL;
    }
    group_info_t *t = g_new0(group_info_t, 1);
    t->name = g_strdup(name);
    t->slaves = g_ptr_array_new();
    return t;
} 

// hash value free
void group_info_free(gpointer n) {
    group_info_t *node = n;
    if (NULL == node) {
        return; 
    }
    guint i;  
    for (i = 0; node->slaves && i < node->slaves->len; i++) {
        slaves_tag_t *slave = g_ptr_array_index(node->slaves, i);
        slaves_tag_free(slave);  
    }
    g_free(node->name);
    network_group_backend_free(node->master);
    g_free(node);
}
void sharding_table_free(gpointer r) {
    sharding_table_t *rule = r;
    if (NULL == rule) {
        return;
    }
    g_string_free(rule->table_name, TRUE);
    g_string_free(rule->shard_key, TRUE);
    if (rule->shard_type == SHARDING_TYPE_RANGE) {
        gint i; 
        for (i = 0; i < rule->shard_groups->len; i++) {
            group_range_map_t *range = &g_array_index(rule->shard_groups, group_range_map_t, 1); 
            g_free(range->group_name);
        }
    } else if (rule->shard_type == SHARDING_TYPE_HASH) {
        gint i; 
        for (i = 0; i < rule->shard_groups->len; i++) {
            group_hash_map_t *hash = &g_array_index(rule->shard_groups, group_hash_map_t, 1); 
            g_free(hash->group_name);
        }

    } else {

    } 
    g_array_free(rule->shard_groups, TRUE);
    g_free(rule);
} 
NETWORK_API network_nodes_t *network_nodes_new(guint event_thread_count, gchar *default_file){
    network_nodes_t *nodes = g_new0(network_nodes_t, 1);
    nodes->default_file = g_strdup(default_file);
    nodes->event_thread_count = event_thread_count;
    nodes->nodes = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, group_info_free); 
    nodes->conn = g_hash_table_new_full(g_str_hash, g_str_equal, g_free,  network_backend_free);
    nodes->raw_ips = g_ptr_array_new_with_free_func(g_free);
    nodes->raw_pwds = g_ptr_array_new_with_free_func(g_free); 
    nodes->group_backends = g_ptr_array_new();
    nodes->forbidden_sql = swap_buffer_new(g_free);
    nodes->sharding_tables = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, sharding_table_free);
    g_mutex_init(&nodes->nodes_mutex);
    return nodes;
}
//TODO(dengyihao):free nodes's other info
NETWORK_API void network_nodes_free(network_nodes_t *nodes){
    if (!nodes) { 
        return; 
    }

    g_mutex_lock(&nodes->nodes_mutex); 
    g_free(nodes->default_file);
    g_free(nodes->default_node);

    // nodes remove and destroy;
    g_hash_table_remove_all(nodes->nodes);
    g_hash_table_destroy(nodes->nodes);

    g_hash_table_remove_all(nodes->conn);
    g_hash_table_destroy(nodes->conn);

    g_ptr_array_free(nodes->raw_ips, TRUE);
    g_ptr_array_free(nodes->raw_pwds, TRUE);
    g_ptr_array_free(nodes->group_backends, TRUE);

    g_free(nodes->router_para);
    swap_buffer_free(nodes->forbidden_sql);
    g_mutex_unlock(&nodes->nodes_mutex);
    g_mutex_clear(&nodes->nodes_mutex);
    g_hash_table_remove_all(nodes->sharding_tables);
    g_hash_table_destroy(nodes->sharding_tables);
    g_free(nodes);
}

void network_nodes_set_default_node(network_nodes_t *nodes, gchar *name){
    if (NULL == nodes || NULL == name) { 
        return; 
    }
    nodes->default_node = g_strdup(name);
}


/*
 *  configure groups backends 
 * */
void set_nodes_backends(network_nodes_t *groups, GPtrArray *arr) {
    if (NULL == groups || NULL == arr) {
        return; 
    } 
    guint i;
    for (i = 0; arr && i < arr->len; ++i) {
        node_info_t *config_node = g_ptr_array_index(arr, i);  // config 
        gchar *group_name = config_node->node_name;

        if (0 == i) { 
            network_nodes_set_default_node(groups, group_name); 
        }

        guint weight;
        gchar *db_addr = get_raw_addr(config_node->master, &weight);
        group_info_t *group = group_info_new(group_name);  

        //TODO(dengyihao):re-coding later to make code elegance
        network_backend_t *backend = g_hash_table_lookup(groups->conn, db_addr);
        if (NULL == backend) {
            backend = network_backend_new(db_addr, groups->event_thread_count);            
            if (NULL != backend) {  
                g_hash_table_insert(groups->conn, g_strdup(db_addr), backend);
            }  
        } 
        if (group && NULL == group->master && NULL != backend) {
            group->master = network_group_backend_new(group_name, NULL, backend, weight, BACKEND_TYPE_RW, 0);
            g_ptr_array_add(groups->group_backends, group->master);
            backend_cited_inc(backend);      
        } 
        g_free(db_addr);

        guint j;
        for (j = 0; config_node->slaves && j < config_node->slaves->len; ++j) {
            slaves_info_t *slave = g_ptr_array_index(config_node->slaves, j); 
            // new slaves_tag init 
            gchar *tag_name = slave->slaves_name;
            slaves_tag_t *taged_slave = slaves_tag_new(tag_name);

            guint k; 
            for (k = 0; slave->ips && k < slave->ips->len; ++k) {
                gchar *ip = g_ptr_array_index(slave->ips, k); 
                guint weight;
                gchar *db_addr = get_raw_addr(ip, &weight); 
                append_idx_by_weight(taged_slave->index, weight, k);

                network_backend_t *backend = g_hash_table_lookup(groups->conn, db_addr);
                if (NULL == backend) {
                    backend = network_backend_new(db_addr, groups->event_thread_count);            
                    if (NULL != backend) {  
                        g_hash_table_insert(groups->conn, g_strdup(db_addr), backend);
                    }  
                } 
                network_group_backend_t *gp_backend = network_group_backend_new(group_name, tag_name, backend, weight, BACKEND_TYPE_RO, k);                
                if (gp_backend) {
                    backend_cited_inc(backend);
                    g_ptr_array_add(groups->group_backends, gp_backend); 
                    g_ptr_array_add(taged_slave->addrs, gp_backend);
                } 
                g_free(db_addr);
            }
            random_shuffle(taged_slave->index);
            g_ptr_array_add(group->slaves, taged_slave);
        } 
        g_hash_table_insert(groups->nodes, g_strdup(group_name), group);
    }
}

void set_dynamic_router_para(dynamic_router_para_t *t, chassis_config_t *config) {
    if (NULL == t || NULL == config) {
        return ;
    }  
    t->read_timeout = config->shutdown_timeout;
    t->sql_safe_update = config->sql_safe_update;
    t->set_router_rule = config->set_router_rule;
    t->auto_sql_filter = config->auto_sql_filter;
    t->max_conn_in_pool = config->max_conn_in_pool;
}

void set_nodes_comm_para(network_nodes_t *n, dynamic_router_para_t *t, GPtrArray *sqls) {
    if (NULL == n || NULL == t) {
        return;
    }
    n->router_para = t;
    set_forbidden_sql(n->forbidden_sql, sqls);
}

//TODO(dengyihao); to add detail sharding value
gboolean node_range_info(gchar *node, group_range_map_t *range) {
    gboolean ret = FALSE;
    gchar *p, *s;
    s = p = node;

    while(*p != 0 && *p != ':') { p++;}  
    if (0 == *p) {
        return ret; 
    }
    *p++ = 0; 
    range->group_name = g_strdup(s);

    s = p;
    while (*p != 0 && *p != '-') { p++;}
    if (*p == 0) {
        g_free(range->group_name);
        return ret; 
    }
    *p++ = 0;

    if (0 == *s) {
        range->range_begin = G_MININT64; 
    } else {
        range->range_begin = atoi(s);
    }

    if (0 == *p) {
        range->range_end = G_MAXINT64;
    } else {
        range->range_end = atoi(p);
    }
    return TRUE;
}
void set_sharding_tables(network_nodes_t *nodes, schema_info_t *schema) {
    if (NULL == nodes || NULL == schema) {
        return;
    }           
    guint i; 
    for (i = 0; i < schema->sharding_rules->len; i++) {
        sharding_rule_t *rule = g_ptr_array_index(schema->sharding_rules, i); 
        if (NULL == rule || NULL == rule->type) { 
            continue; 
        }
        gchar *key = g_strdup_printf("%s_%s", rule->db_name, rule->table); 

        sharding_table_t *sharding_table = g_new0(sharding_table_t, 1); 
        sharding_table->auto_inc = rule->auto_inc; // parameter 
        sharding_table->primary_key = g_string_new(rule->primary_key);
        sharding_table->shard_key = g_string_new(rule->sharding_key);
        sharding_table->table_name = g_string_new(rule->table);

        if (0 == g_ascii_strcasecmp(rule->type, "HASH")) {
            sharding_table->shard_type = SHARDING_TYPE_HASH;
            sharding_table->shard_groups = g_array_new(FALSE, TRUE, sizeof(group_hash_map_t));
            gint j;
            for (j = 0; j < rule->nodes->len; j++) {
                group_hash_map_t hash;
                hash.group_name = g_strdup(g_ptr_array_index(rule->nodes, j));
                g_array_append_val(sharding_table->shard_groups, hash);
            } 
            //TODO(dengyihao): to add detail later
        } else if(0 == g_ascii_strcasecmp(rule->type, "RANGE")) {
            sharding_table->shard_type = SHARDING_TYPE_RANGE;
            sharding_table->shard_groups = g_array_new(FALSE, TRUE, sizeof(group_range_map_t)); 
            gint j; 
            for (j = 0; j < rule->nodes->len; j++) {
                gchar *node = g_ptr_array_index(rule->nodes, j);
                group_range_map_t range;
                range.group_index = j;
                if (FALSE == node_range_info(node, &range)) {
                    continue; // or return 
                } 

                g_array_append_val(sharding_table->shard_groups, range);
            }
            //TODO(dengyihao): to add detail later 
        } else {
            sharding_table->shard_type = SHARDING_TYPE_UNKOWN;
        }
        g_hash_table_insert(nodes->sharding_tables, key, sharding_table);
    } 
}
/*
 *
 * save user:pwd info 
 */
void save_user_info(network_nodes_t *t, gchar *user, gchar *pwd){
    if (NULL == t || NULL == t->raw_pwds|| NULL == user || NULL == pwd) { 
        return; 
    }
    g_ptr_array_add(t->raw_pwds, g_strdup_printf("%s:%s", user, pwd));
}
/*
 *
 * save user ip 
 */
void save_client_ip(network_nodes_t *t, gchar *client_ip) {
    if (NULL == t || NULL == client_ip) {
        return;
    }
    g_ptr_array_add(t->raw_ips, g_strdup(client_ip));
}


void set_forbidden_sql(swap_buffer_t *buffer, GPtrArray *forbidden_sql) {
    if (NULL == buffer || NULL == forbidden_sql) {
        return;
    }   
    gint i;
    for (i = 0; i < forbidden_sql->len; i++) {
        gchar *s = forbidden_sql->pdata[i];
        if (g_hash_table_contains(buffer->sets, s)) {
            continue;
        }
        gchar *s_dup = g_strdup(s);
        g_ptr_array_add(buffer->arr[*(buffer->idx)], s_dup);
        g_hash_table_add(buffer->sets, s_dup);
    }
}


/*
 * swap buffer(ping-pong buffer) 
 * operation func include new/detete/add/free
 */
swap_buffer_t *swap_buffer_new(void (*destroy)(gpointer data)) {
    swap_buffer_t *t = g_new0(swap_buffer_t, 1);         
    t->arr[0] = g_ptr_array_new_with_free_func(destroy); 
    t->arr[1] = g_ptr_array_new_with_free_func(destroy); 
    t->idx = g_new0(int, 1);
    t->sets = g_hash_table_new(g_str_hash, g_str_equal);
    g_atomic_int_set(t->idx, 0);  // init value
    return t;
}
void swap_buffer_free(swap_buffer_t *t) {
    if (NULL == t) {
        return;
    }
    g_ptr_array_free(t->arr[0], TRUE);
    g_ptr_array_free(t->arr[1], TRUE);
    g_free(t->idx);
    g_hash_table_destroy(t->sets);
    g_free(t);
} 

void copy_element(gpointer v, gpointer arr) {
    gchar *data = v;
    GPtrArray *user_data = arr;
    if (NULL == data || NULL == user_data) {
        return;
    }
    g_ptr_array_add(user_data, g_strdup(data));
} 

gchar *swap_buffer_get(swap_buffer_t *t, gint index) {
    if (NULL == t) {
        return NULL;
    }
    gint idx = *(t->idx);
    return t->arr[idx]->pdata[index];
} 

gboolean swap_buffer_add(swap_buffer_t *t, gchar *ele) {
    gboolean ret = FALSE;
    if (NULL == t || NULL == ele)  {
        return ret;
    } 
    if (g_hash_table_contains(t->sets, ele)) {
        return ret;
    }
    gint index = *(t->idx);
    GPtrArray *old_arr = t->arr[index];
    GPtrArray *new_arr = t->arr[1 - index];

    g_ptr_array_set_size(new_arr, 0);
    g_ptr_array_foreach(old_arr, copy_element, new_arr);

    gchar *str = g_strdup(ele); 
    g_hash_table_add(t->sets, str);
    g_ptr_array_add(new_arr, str); 

    g_atomic_int_set(t->idx, 1 - index);
    ret = TRUE;
    return ret;

}

gboolean swap_buffer_remove(swap_buffer_t *t, gint i) {
    gboolean ret = FALSE; 
    if (NULL == t) {
        return ret;
    }

    gint index = *(t->idx);
    GPtrArray *old_arr = t->arr[index];
    GPtrArray *new_arr = t->arr[1 - index];

    g_ptr_array_set_size(new_arr, 0);
    g_ptr_array_foreach(old_arr, copy_element, new_arr);

    gchar *str = new_arr->pdata[i];
    g_hash_table_remove(t->sets, str);
    g_ptr_array_remove(new_arr, str); 

    g_atomic_int_set(t->idx, 1 - index);
    ret = TRUE;
    return ret;
}

GPtrArray *swap_buffer_in_use(swap_buffer_t *t) {
    GPtrArray *ret = NULL;
    if (NULL == t) {
        return ret;
    }
    gint idx = *(t->idx);
    return t->arr[idx];
}

sharding_table_t *sharding_lookup_table_rule(GHashTable *sharding_tables, parse_info_t *parse_info, gchar *default_db, gchar **full_table_name) {
    if (NULL == sharding_tables || NULL == parse_info || NULL == parse_info->table_list) {
        return NULL;
    } 
    SrcList *srclist = parse_info->table_list;
    sharding_table_t *shard_rule = NULL;
    guint i; 
    for (i = 0; i < srclist->nSrc; i++) {
        const char* db_name = srclist->a[i].zDatabase;
        const char* table_name = srclist->a[i].zName;
        if (db_name == NULL) {
            *full_table_name = g_strdup_printf("%s_%s", default_db, table_name);
        } else {
            *full_table_name = g_strdup_printf("%s_%s", db_name, table_name);
        }
        shard_rule = (sharding_table_t*)g_hash_table_lookup(sharding_tables, *full_table_name);

        //if (full_table_name) { g_free(full_table_name);  }

        if (shard_rule != NULL) { break;  }

    }
    return shard_rule;
}  
