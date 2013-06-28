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
#include <stdlib.h>

#include "glib-ext.h"
#include "network-mysqld-masterinfo.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

network_mysqld_masterinfo_t * network_mysqld_masterinfo_new(void) {
	network_mysqld_masterinfo_t *info;

	info = g_new0(network_mysqld_masterinfo_t, 1);

	info->master_log_file   = g_string_new(NULL);
	info->master_host       = g_string_new(NULL);
	info->master_user       = g_string_new(NULL);
	info->master_password   = g_string_new(NULL);
	
	info->master_ssl_ca     = g_string_new(NULL);
	info->master_ssl_capath = g_string_new(NULL);
	info->master_ssl_cert   = g_string_new(NULL);
	info->master_ssl_cipher = g_string_new(NULL);
	info->master_ssl_key    = g_string_new(NULL);

	return info;
}

void network_mysqld_masterinfo_free(network_mysqld_masterinfo_t *info) {
	if (!info) return;

	g_string_free(info->master_log_file, TRUE);
	g_string_free(info->master_host, TRUE);
	g_string_free(info->master_user, TRUE);
	g_string_free(info->master_password, TRUE);
	
	g_string_free(info->master_ssl_ca, TRUE);
	g_string_free(info->master_ssl_capath, TRUE);
	g_string_free(info->master_ssl_cert, TRUE);
	g_string_free(info->master_ssl_cipher, TRUE);
	g_string_free(info->master_ssl_key, TRUE);

	g_free(info);
}

/**
 * get \n terminated strings 
 */

static int network_mysqld_masterinfo_get_string(network_packet *packet, GString *str) {
	guint i;

	g_return_val_if_fail(packet, -1);
	g_return_val_if_fail(str, -1);

	for (i = packet->offset; i < packet->data->len; i++) {
		const char c = packet->data->str[i];

		if (c == '\n') break;
	}

	if (packet->data->str[i] == '\n') {
		g_string_assign_len(str, packet->data->str + packet->offset, i - packet->offset);

		packet->offset = i + 1; /* start the next string after our \n */

		return 0;
	} 

	return -1;
}

static int network_mysqld_masterinfo_get_int32(network_packet *packet, guint32 *_i) {
	GString *s;
	int err = 0;

	s = g_string_new(NULL);
	err = err || network_mysqld_masterinfo_get_string(packet, s);
	if (!err) {
		char *errptr;
		guint32 i;

		i = strtoul(s->str, &errptr, 0);

		err = err || (*errptr != '\0');

		if (!err) *_i = i;
	}

	g_string_free(s, TRUE);

	return err ? -1 : 0;
}

/**
 * get the master-info structure from the internal representation 
 */
int network_mysqld_masterinfo_get(network_packet *packet, network_mysqld_masterinfo_t *info) {
	int err = 0;

	g_return_val_if_fail(info, -1);
	g_return_val_if_fail(packet, -1);

	/*err = err || network_mysqld_masterinfo_get_int32(packet, &lines);*/
	/*info->master_lines = lines;*/
        err = err || network_mysqld_masterinfo_get_int32(packet, &(info->master_lines));
        err = err || network_mysqld_masterinfo_get_string(packet, info->master_log_file);
	err = err || network_mysqld_masterinfo_get_int32(packet, &(info->master_log_pos));
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_host);
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_user);
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_password);
	err = err || network_mysqld_masterinfo_get_int32(packet, &(info->master_port));
	err = err || network_mysqld_masterinfo_get_int32(packet, &(info->master_connect_retry));
	err = err || network_mysqld_masterinfo_get_int32(packet, &(info->master_ssl));
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_ssl_ca);
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_ssl_capath);
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_ssl_cert);
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_ssl_cipher);
	err = err || network_mysqld_masterinfo_get_string(packet, info->master_ssl_key);
	if (info->master_lines >= 15) {
		err = err || network_mysqld_masterinfo_get_int32(packet, &(info->master_ssl_verify_server_cert));
	}
	return err ? -1 : 0;
}

static int network_mysqld_masterinfo_append_string(GString *packet, GString *s) {
	g_string_append_len(packet, S(s));
	g_string_append_c(packet, '\n');

	return 0;
}

static int network_mysqld_masterinfo_append_int32(GString *packet, guint32 i) {
	g_string_append_printf(packet, "%"G_GUINT32_FORMAT"\n", i);

	return 0;
}


int network_mysqld_masterinfo_append(GString *packet, network_mysqld_masterinfo_t *info) {
	int err = 0;

	g_return_val_if_fail(info, -1);
	g_return_val_if_fail(packet, -1);

	err = err || network_mysqld_masterinfo_append_int32(packet, info->master_lines);
        err = err || network_mysqld_masterinfo_append_string(packet, info->master_log_file);
	err = err || network_mysqld_masterinfo_append_int32(packet, info->master_log_pos);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_host);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_user);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_password);
	err = err || network_mysqld_masterinfo_append_int32(packet, info->master_port);
	err = err || network_mysqld_masterinfo_append_int32(packet, info->master_connect_retry);
	err = err || network_mysqld_masterinfo_append_int32(packet, info->master_ssl);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_ssl_ca);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_ssl_capath);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_ssl_cert);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_ssl_cipher);
	err = err || network_mysqld_masterinfo_append_string(packet, info->master_ssl_key);
	if (info->master_lines >= 15) {
                err = err || network_mysqld_masterinfo_append_int32(packet, info->master_ssl_verify_server_cert);
        }

	return err ? -1 : 0;
}

