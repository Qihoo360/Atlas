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

#include <glib.h>

#include "network-mysqld-packet.h"
#include "network-backend.h"
#include "chassis-plugin.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

network_backend_t *network_backend_new(guint event_thread_count) {
	network_backend_t *b = g_new0(network_backend_t, 1);

	b->pools = g_ptr_array_new();
	guint i;
	for (i = 0; i <= event_thread_count; ++i) {
		network_connection_pool* pool = network_connection_pool_new();
		g_ptr_array_add(b->pools, pool);
	}

	b->uuid = g_string_new(NULL);
	b->addr = network_address_new();

	return b;
}

void network_backend_free(network_backend_t *b) {
	if (!b) return;

	guint i;
	for (i = 0; i < b->pools->len; ++i) {
		network_connection_pool* pool = g_ptr_array_index(b->pools, i);
		network_connection_pool_free(pool);
	}
	g_ptr_array_free(b->pools, TRUE);

	if (b->addr)     network_address_free(b->addr);
	if (b->uuid)     g_string_free(b->uuid, TRUE);

	g_free(b);
}

network_backends_t *network_backends_new(guint event_thread_count, gchar *default_file) {
	network_backends_t *bs;

	bs = g_new0(network_backends_t, 1);

	bs->backends = g_ptr_array_new();
	bs->backends_mutex = g_mutex_new();	/*remove lock*/
	bs->global_wrr = g_wrr_poll_new();
	bs->event_thread_count = event_thread_count;
	bs->default_file = g_strdup(default_file);

	return bs;
}

g_wrr_poll *g_wrr_poll_new() {
    g_wrr_poll *global_wrr;

    global_wrr = g_new0(g_wrr_poll, 1);

    global_wrr->max_weight = 0;
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

	g_mutex_lock(bs->backends_mutex);	/*remove lock*/
	for (i = 0; i < bs->backends->len; i++) {
		network_backend_t *backend = bs->backends->pdata[i];
		
		network_backend_free(backend);
	}
	g_mutex_unlock(bs->backends_mutex);	/*remove lock*/

	g_ptr_array_free(bs->backends, TRUE);
	g_mutex_free(bs->backends_mutex);	/*remove lock*/

	g_wrr_poll_free(bs->global_wrr);
	g_free(bs->default_file);

	g_free(bs);
}

int network_backends_remove(network_backends_t *bs, guint index) {
	network_backend_t* b = bs->backends->pdata[index];
	if (b != NULL) {
		if (b->addr) network_address_free(b->addr);
		if (b->uuid) g_string_free(b->uuid, TRUE);
		g_mutex_lock(bs->backends_mutex);
		g_ptr_array_remove_index(bs->backends, index);
		g_mutex_unlock(bs->backends_mutex);
	}
	return 0;
}

void copy_key(guint *key, guint *value, GHashTable *table) {
	guint *new_key = g_new0(guint, 1);
	*new_key = *key;
	g_hash_table_add(table, new_key);
}

void copy_pwd(gchar *key, GString *value, GHashTable *table) {
	g_hash_table_insert(table, g_strdup(key), g_string_new_len(S(value)));
}

int network_backends_addclient(network_backends_t *bs, gchar *address) {
	guint* sum = g_new0(guint, 1);
	char* token;
	while ((token = strsep(&address, ".")) != NULL) {
		*sum = (*sum << 8) + atoi(token);
	}
	*sum = htonl(*sum);

	gint index = *(bs->ip_table_index);
	GHashTable *old_table = bs->ip_table[index];
	GHashTable *new_table = bs->ip_table[1-index];
	g_hash_table_remove_all(new_table);
	g_hash_table_foreach(old_table, copy_key, new_table);
	g_hash_table_add(new_table, sum);
	g_atomic_int_set(bs->ip_table_index, 1-index);

	return 0;
}

int network_backends_addpwd(network_backends_t *bs, gchar *address) {
	char *user = NULL, *pwd = NULL;
	gboolean is_complete = FALSE;

	if ((user = strsep(&address, ":")) != NULL) {
		if ((pwd = strsep(&address, ":")) != NULL) {
			is_complete = TRUE;
		}
	}

	if (is_complete == FALSE) {
		g_warning("incorrect password settings");
		return -1;
	}

	GString* hashed_password = g_string_new(NULL);
	network_mysqld_proto_password_hash(hashed_password, pwd, strlen(pwd));

	gint index = *(bs->pwd_table_index);
	GHashTable *old_table = bs->pwd_table[index];
	GHashTable *new_table = bs->pwd_table[1-index];
	g_hash_table_remove_all(new_table);
	g_hash_table_foreach(old_table, copy_pwd, new_table);
	g_hash_table_insert(new_table, g_strdup(user), hashed_password);
	g_atomic_int_set(bs->pwd_table_index, 1-index);

	return 0;
}

int network_backends_removeclient(network_backends_t *bs, gchar *address) {
	guint sum;
	char* token;
	while ((token = strsep(&address, ".")) != NULL) {
		sum = (sum << 8) + atoi(token);
	}
	sum = htonl(sum);

	gint index = *(bs->ip_table_index);
	GHashTable *old_table = bs->ip_table[index];
	GHashTable *new_table = bs->ip_table[1-index];
	g_hash_table_remove_all(new_table);
	g_hash_table_foreach(old_table, copy_key, new_table);
	g_hash_table_remove(new_table, &sum);
	g_atomic_int_set(bs->ip_table_index, 1-index);

	return 0;
}

int network_backends_removepwd(network_backends_t *bs, gchar *address) {
	gint index = *(bs->pwd_table_index);
	GHashTable *old_table = bs->pwd_table[index];
	GHashTable *new_table = bs->pwd_table[1-index];
	g_hash_table_remove_all(new_table);
	g_hash_table_foreach(old_table, copy_pwd, new_table);
	g_hash_table_remove(new_table, address);
	g_atomic_int_set(bs->pwd_table_index, 1-index);

	return 0;
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

int network_backends_save(network_backends_t *bs) {
	GKeyFile *keyfile = g_key_file_new();
	g_key_file_set_list_separator(keyfile, ',');
	GError *gerr = NULL;

	if (FALSE == g_key_file_load_from_file(keyfile, bs->default_file, G_KEY_FILE_KEEP_COMMENTS, &gerr)) {
		g_critical("%s: g_key_file_load_from_file: %s", G_STRLOC, gerr->message);
		g_error_free(gerr);
		g_key_file_free(keyfile);
		return -1;
	}

	GString *master = g_string_new(NULL);
	GString *slave  = g_string_new(NULL);
	guint i;
	GPtrArray *backends = bs->backends;

	g_mutex_lock(bs->backends_mutex);
	guint len = backends->len;
	for (i = 0; i < len; ++i) {
		network_backend_t *backend = g_ptr_array_index(backends, i);
		if (backend->type == BACKEND_TYPE_RW) {
			g_string_append_c(master, ',');
			g_string_append(master, backend->addr->name->str);
		} else if (backend->type == BACKEND_TYPE_RO) {
			g_string_append_c(slave, ',');
			g_string_append(slave, backend->addr->name->str);
		}
	}
	g_mutex_unlock(bs->backends_mutex);

	if (master->len != 0) {
		g_key_file_set_value(keyfile, "mysql-proxy", "proxy-backend-addresses", master->str+1);
	} else {
		g_key_file_set_value(keyfile, "mysql-proxy", "proxy-backend-addresses", "");
	}
	if (slave->len != 0) {
		g_key_file_set_value(keyfile, "mysql-proxy", "proxy-read-only-backend-addresses", slave->str+1);
	} else {
		g_key_file_set_value(keyfile, "mysql-proxy", "proxy-read-only-backend-addresses", "");
	}

	g_string_free(master, TRUE);
	g_string_free(slave, TRUE);

	GString *client_ips = g_string_new(NULL);
	GHashTable *ip_table = bs->ip_table[*(bs->ip_table_index)];
	g_hash_table_foreach(ip_table, append_key, client_ips);

	if (client_ips->len != 0) {
		g_key_file_set_value(keyfile, "mysql-proxy", "client-ips", client_ips->str+1);
	} else {
		g_key_file_set_value(keyfile, "mysql-proxy", "client-ips", "");
	}

	g_string_free(client_ips, TRUE);

	gsize file_size = 0;
	gchar *file_buf = g_key_file_to_data(keyfile, &file_size, NULL);
	if (FALSE == g_file_set_contents(bs->default_file, file_buf, file_size, &gerr)) {
		g_critical("%s: g_file_set_contents: %s", G_STRLOC, gerr->message);
		g_free(file_buf);
		g_error_free(gerr);
		g_key_file_free(keyfile);
		return -1;
	}

	g_message("%s: saving config file succeed", G_STRLOC);
	g_free(file_buf);
	g_key_file_free(keyfile);
	return 0;
}

/*
 * FIXME: 1) remove _set_address, make this function callable with result of same
 *        2) differentiate between reasons for "we didn't add" (now -1 in all cases)
 */
int network_backends_add(network_backends_t *bs, /* const */ gchar *address, backend_type_t type) {
	network_backend_t *new_backend;
	guint i;

	new_backend = network_backend_new(bs->event_thread_count);
	new_backend->type = type;

	gchar *p = NULL;
	if (type == BACKEND_TYPE_RO) {
		guint weight = 1;
		p = strrchr(address, '@');
		if (p != NULL) {
			*p = '\0';
			weight = atoi(p+1);
		}
		new_backend->weight = weight;
	}

	if (0 != network_address_set_address(new_backend->addr, address)) {
		network_backend_free(new_backend);
		return -1;
	}

	/* check if this backend is already known */
	g_mutex_lock(bs->backends_mutex);	/*remove lock*/
	gint first_slave = -1;
	for (i = 0; i < bs->backends->len; i++) {
		network_backend_t *old_backend = bs->backends->pdata[i];

		if (first_slave == -1 && old_backend->type == BACKEND_TYPE_RO) first_slave = i;

		if (old_backend->type == type && strleq(S(old_backend->addr->name), S(new_backend->addr->name))) {
			network_backend_free(new_backend);

			g_mutex_unlock(bs->backends_mutex);	/*remove lock*/
			g_critical("backend %s is already known!", address);
			return -1;
		}
	}

	g_ptr_array_add(bs->backends, new_backend);
	if (first_slave != -1 && type == BACKEND_TYPE_RW) {
		network_backend_t *temp_backend = bs->backends->pdata[first_slave];
		bs->backends->pdata[first_slave] = bs->backends->pdata[bs->backends->len - 1];
		bs->backends->pdata[bs->backends->len - 1] = temp_backend;
	}
	g_mutex_unlock(bs->backends_mutex);	/*remove lock*/

	g_message("added %s backend: %s", (type == BACKEND_TYPE_RW) ? "read/write" : "read-only", address);

	if (p != NULL) *p = '@';

	return 0;
}

network_backend_t *network_backends_get(network_backends_t *bs, guint ndx) {
	if (ndx >= network_backends_count(bs)) return NULL;

	/* FIXME: shouldn't we copy the backend or add ref-counting ? */	
	return bs->backends->pdata[ndx];
}

guint network_backends_count(network_backends_t *bs) {
	guint len;

	g_mutex_lock(bs->backends_mutex);	/*remove lock*/
	len = bs->backends->len;
	g_mutex_unlock(bs->backends_mutex);	/*remove lock*/

	return len;
}

