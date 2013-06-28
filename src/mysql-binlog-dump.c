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
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include <glib/gstdio.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <lua.h>

#include "glib-ext.h"
#include "chassis-mainloop.h"
#include "chassis-log.h"
#include "chassis-keyfile.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-binlog.h"

#define S(x) x->str, x->len


/* forward decl */
const char *network_mysqld_proto_field_get_typestring(enum enum_field_types type);

typedef struct {
	MYSQL_FIELD *fielddef;

	union {
		guint64 i;
		gchar *s;
		double d;
	} data;
	guint64 data_len;

	gboolean is_null;
} network_mysqld_proto_field;

network_mysqld_proto_field *network_mysqld_proto_field_new() {
	network_mysqld_proto_field *field;

	field = g_new0(network_mysqld_proto_field, 1);

	return field;
}

void network_mysqld_proto_field_free(network_mysqld_proto_field *field) {
	if (!field) return;

	switch ((guchar)field->fielddef->type) {
	case MYSQL_TYPE_TIMESTAMP:
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_YEAR:

	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_LONGLONG:

	case MYSQL_TYPE_DECIMAL:
	case MYSQL_TYPE_NEWDECIMAL:

	case MYSQL_TYPE_ENUM:
	case MYSQL_TYPE_SET:

	case MYSQL_TYPE_DOUBLE:
	case MYSQL_TYPE_FLOAT:
		break;
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_GEOMETRY:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
		if (field->data.s) g_free(field->data.s);
		break;
	default:
		g_message("%s: unknown field_type '%s' (%d) to free()",
				G_STRLOC,
				network_mysqld_proto_field_get_typestring(field->fielddef->type),
				field->fielddef->type);
		break;
	}

	g_free(field);
}

int network_mysqld_proto_field_get(network_packet *packet, 
		network_mysqld_proto_field *field) {
	guint64 length;
	guint8  i8;
	guint16 i16;
	guint32 i32;
	guint64 i64;
	int err = 0;
	gchar *s;

	switch ((guchar)field->fielddef->type) {
	case MYSQL_TYPE_BIT:
		err = err || network_mysqld_proto_get_string_len(packet, &field->data.s, field->fielddef->max_length);
		break;
	case MYSQL_TYPE_FLOAT: /* float4store */
		s = NULL;
		err = err || network_mysqld_proto_get_string_len(packet, &s, 4);
		if (!err) {
			union {
				float d;
				guchar s[4];
			} shadow;

			guint i;

			memcpy(shadow.s, s, 4);

#if 0
			/* on BIG-ENDIAN we may have to swap the bytes */
			for (i = 0; i < 4 / 2; i++) {
				guchar tmp;

				tmp = shadow.s[i];
				shadow.s[i] = shadow.s[3 - i];
				shadow.s[3 - i] = tmp;
			}
#endif
			field->data.d = shadow.d;
		}
		g_free(s);
		break;
	case MYSQL_TYPE_DOUBLE: /* float8store */
		s = NULL;
		err = err || network_mysqld_proto_get_string_len(packet, &s, 8);
		if (!err) {
			union {
				double d;
				guchar s[8];
			} shadow;

			guint i;

			memcpy(shadow.s, s, 8);

#if 0
			/* on BIG-ENDIAN we may have to swap the bytes */
			for (i = 0; i < 8 / 2; i++) {
				guchar tmp;

				tmp = shadow.s[i];
				shadow.s[i] = shadow.s[7 - i];
				shadow.s[7 - i] = tmp;
			}
#endif
			field->data.d = shadow.d;
		}
		g_free(s);
		break;
	case MYSQL_TYPE_TIMESTAMP: /* int4store */
	case MYSQL_TYPE_LONG:
		err = err || network_mysqld_proto_get_int32(packet, &i32);
		if (!err) field->data.i = i32;
		break;
	case MYSQL_TYPE_DATETIME: /* int8store */
	case MYSQL_TYPE_LONGLONG:
		err = err || network_mysqld_proto_get_int64(packet, &i64);
		if (!err) field->data.i = i64;
		break;
	case MYSQL_TYPE_INT24:     
	case MYSQL_TYPE_DATE:      /* int3store, a newdate, old-data is 4 byte */
	case MYSQL_TYPE_TIME:      /* int3store */
		err = err || network_mysqld_proto_get_int24(packet, &i32);
		if (!err) field->data.i = i32;
		break;
	case MYSQL_TYPE_SHORT:     
		err = err || network_mysqld_proto_get_int16(packet, &i16);
		if (!err) field->data.i = i16;
		break;
	case MYSQL_TYPE_YEAR:
	case MYSQL_TYPE_TINY:     
		err = err || network_mysqld_proto_get_int8(packet, &i8);
		if (!err) field->data.i = i8;
		break;
	case MYSQL_TYPE_SET:
	case MYSQL_TYPE_ENUM:
		switch (field->fielddef->max_length) {
		case 1:
			err = err || network_mysqld_proto_get_int8(packet, &i8);
			if (!err) field->data.i = i8;
			break;
		case 2:
			err = err || network_mysqld_proto_get_int16(packet, &i16);
			if (!err) field->data.i = i16;
			break;
		default:
			g_error("%s: enum-length = %lu", 
					G_STRLOC,
					field->fielddef->max_length);
			break;
		}
		break;
	case MYSQL_TYPE_GEOMETRY:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
		switch (field->fielddef->max_length) {
		case 1:
			err = err || network_mysqld_proto_get_int8(packet, &i8);
			if (!err) length = i8;
			break;
		case 2:
			err = err || network_mysqld_proto_get_int16(packet, &i16);
			if (!err) length = i16;
			break;
		case 3:
			err = err || network_mysqld_proto_get_int24(packet, &i32);
			if (!err) length = i32;
			break;
		case 4:
			err = err || network_mysqld_proto_get_int32(packet, &i32);
			if (!err) length = i32;
			break;
		default:
			/* unknown blob-length */
			g_debug_hexdump(G_STRLOC, S(packet->data));
			g_error("%s: blob-length = %lu", 
					G_STRLOC,
					field->fielddef->max_length);
			break;
		}
		err = err || network_mysqld_proto_get_string_len(packet, &field->data.s, length);
		break;
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
		if (field->fielddef->max_length < 256) {
			err = err || network_mysqld_proto_get_int8(packet, &i8);
			err = err || network_mysqld_proto_get_string_len(packet, &field->data.s, i8);
		} else {
			err = err || network_mysqld_proto_get_int16(packet, &i16);
			err = err || network_mysqld_proto_get_string_len(packet, &field->data.s, i16);
		}

		break;
	case MYSQL_TYPE_NEWDECIMAL: {
		/* the decimal is binary encoded
		 */
		guchar digits_per_bytes[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 }; /* how many bytes are needed to store x decimal digits */

		guint i_digits = field->fielddef->max_length - field->fielddef->decimals;
		guint f_digits = field->fielddef->decimals;

		guint decimal_full_blocks       = i_digits / 9; /* 9 decimal digits in 4 bytes */
		guint decimal_last_block_digits = i_digits % 9; /* how many digits are left ? */

		guint scale_full_blocks         = f_digits / 9; /* 9 decimal digits in 4 bytes */
		guint scale_last_block_digits   = f_digits % 9; /* how many digits are left ? */

		guint size = 0;

		size += decimal_full_blocks * digits_per_bytes[9] + digits_per_bytes[decimal_last_block_digits];
		size += scale_full_blocks   * digits_per_bytes[9] + digits_per_bytes[scale_last_block_digits];

#if 0
		g_debug_hexdump(G_STRLOC " (NEWDECIMAL)", packet->data->str, packet->data->len);
#endif
#if 0
		g_critical("%s: don't know how to decode NEWDECIMAL(%lu, %u) at offset %u (%d)",
				G_STRLOC,
				field->fielddef->max_length,
				field->fielddef->decimals,
				packet->offset,
				size
				);
#endif
		err = err || network_mysqld_proto_skip(packet, size);
		break; }
	default:
		g_debug_hexdump(G_STRLOC, packet->data->str, packet->data->len);
		g_error("%s: unknown field-type '%d' to fetch from offset = %04x",
				G_STRLOC,
				field->fielddef->type,
				packet->offset);
		break;
	}

	return err ? -1 : 0;
}

/**
 * get the field-definitions of columns that are included in this log-event
 */
GPtrArray *network_mysqld_proto_fields_new_full(
		GPtrArray *fielddefs,
		gchar *used_cols_bits,
		guint G_GNUC_UNUSED used_cols_bits_len,
		gchar *null_bits,
		guint G_GNUC_UNUSED null_bits_len) {
	GPtrArray *fields;
	guint col;
	guint null_bit;

	fields = g_ptr_array_new();

	for (col = 0, null_bit = 0; col < fielddefs->len; col++) {
		MYSQL_FIELD *fielddef = fielddefs->pdata[col];

		guint col_byteoffset = col / 8;
		guint col_bitoffset = col % 8;

		/* check if we have data for this columns in the row-log-event */
		if ((used_cols_bits[col_byteoffset] >> col_bitoffset) & 0x1) {
			network_mysqld_proto_field *field = network_mysqld_proto_field_new();

			guint null_byteoffset = null_bit / 8;
			guint null_bitoffset = null_bit % 8;

			field->fielddef = fielddef;
			field->is_null = (null_bits[null_byteoffset] >> null_bitoffset) & 0x1;

			/* the field is defined as NOT NULL, so the null-bit shouldn't be set */
			if ((fielddef->flags & NOT_NULL_FLAG) != 0) {
				if (field->is_null) {
					g_critical("%s: field [%d] is defined as NOT NULL, but nul-bit is set",
							G_STRLOC,
							col);

					return NULL;
				}
			}

			null_bit++;

			g_ptr_array_add(fields, field);
		}
	}

	return fields;
}

int network_mysqld_proto_fields_get(network_packet *packet, GPtrArray *fields) {
	guint i;

	for (i = 0; i < fields->len; i++) {
		network_mysqld_proto_field *field = fields->pdata[i];

		if (!field->is_null) {
			if (network_mysqld_proto_field_get(packet, field)) return -1;
		}
	}

	return 0;
}

void network_mysqld_proto_fields_free(GPtrArray *fields) {
	guint i;
	if (!fields) return;

	for (i = 0; i < fields->len; i++) {
		network_mysqld_proto_field_free(fields->pdata[i]);
	}
	g_ptr_array_free(fields, TRUE);
}

struct {
	enum Log_event_type type;
	const char *name;
} event_type_name[] = {
#define V(x) x, #x
	{ V(UNKNOWN_EVENT) },
	{ V(START_EVENT_V3) },
	{ V(QUERY_EVENT) },
	{ V(STOP_EVENT) },
	{ V(ROTATE_EVENT) },
	{ V(INTVAR_EVENT) },
	{ V(LOAD_EVENT) },
	{ V(SLAVE_EVENT) },
	{ V(CREATE_FILE_EVENT) },
	{ V(APPEND_BLOCK_EVENT) },
	{ V(EXEC_LOAD_EVENT) },
	{ V(DELETE_FILE_EVENT) },
	{ V(NEW_LOAD_EVENT) },
	{ V(RAND_EVENT) },
	{ V(USER_VAR_EVENT) },
	{ V(FORMAT_DESCRIPTION_EVENT) },
	{ V(XID_EVENT) },
	{ V(BEGIN_LOAD_QUERY_EVENT) },
	{ V(EXECUTE_LOAD_QUERY_EVENT) },
	{ V(TABLE_MAP_EVENT ) },
	{ V(PRE_GA_WRITE_ROWS_EVENT ) },
	{ V(PRE_GA_UPDATE_ROWS_EVENT ) },
	{ V(PRE_GA_DELETE_ROWS_EVENT ) },
	{ V(WRITE_ROWS_EVENT ) },
	{ V(UPDATE_ROWS_EVENT ) },
	{ V(DELETE_ROWS_EVENT ) },
	{ V(INCIDENT_EVENT) },
	{ V(HEARTBEAT_LOG_EVENT) },
	{ V(IGNORABLE_LOG_EVENT) },
	{ V(ROWS_QUERY_LOG_EVENT) },

#undef V
	{ 0, NULL }
};

const char *network_mysqld_binlog_get_eventname(enum Log_event_type type) {
	static const char *unknown_type = "UNKNOWN";
	guint i;

	for (i = 0; event_type_name[i].name; i++) {
		if ((guchar)event_type_name[i].type == (guchar)type) return event_type_name[i].name;
	}

	g_critical("%s: event-type %d isn't known yet", 
			G_STRLOC,
			type);

	return unknown_type;
}

struct {
	enum enum_field_types type;
	const char *name;
} field_type_name[] = {
	{ MYSQL_TYPE_STRING, "CHAR" },
	{ MYSQL_TYPE_VARCHAR, "VARCHAR" },
	{ MYSQL_TYPE_VAR_STRING, "VARCHAR /* varstring */" },
	{ MYSQL_TYPE_TINY_BLOB, "TINYBLOB" },
	{ MYSQL_TYPE_MEDIUM_BLOB, "MEDIUMBLOB" },
	{ MYSQL_TYPE_BLOB, "BLOB" },
	{ MYSQL_TYPE_LONG_BLOB, "LONGBLOB" },

	{ MYSQL_TYPE_TINY, "TINYINT" },
	{ MYSQL_TYPE_SHORT, "SMALLINT" },
	{ MYSQL_TYPE_INT24, "MEDIUMINT" },
	{ MYSQL_TYPE_LONG, "INT" },
	{ MYSQL_TYPE_LONGLONG, "BIGINT" },
	{ MYSQL_TYPE_NEWDECIMAL, "DECIMAL" },

	{ MYSQL_TYPE_ENUM, "ENUM" },
	{ MYSQL_TYPE_SET, "SET" },

	{ MYSQL_TYPE_TIMESTAMP, "TIMESTAMP" },
	{ MYSQL_TYPE_DATE, "DATE" },
	{ MYSQL_TYPE_DATETIME, "DATETIME" },
	{ MYSQL_TYPE_TIME, "TIME" },
	{ MYSQL_TYPE_YEAR, "YEAR" },
	{ MYSQL_TYPE_NEWDATE, "DATE /* new */" },

	{ MYSQL_TYPE_FLOAT, "FLOAT" },
	{ MYSQL_TYPE_DOUBLE, "DOUBLE" },
	{ MYSQL_TYPE_DECIMAL, "DECIMAL /* old */" },

	{ MYSQL_TYPE_NULL, "NULL" },
	{ MYSQL_TYPE_BIT, "BIT" },

	{ MYSQL_TYPE_GEOMETRY, "GEOMETRY" },

	{ 0, NULL }
};

const char *network_mysqld_proto_field_get_typestring(enum enum_field_types type) {
	static const char *unknown_type = "UNKNOWN";
	guint i;

	for (i = 0; field_type_name[i].name; i++) {
		if ((guchar)field_type_name[i].type == (guchar)type) return field_type_name[i].name;
	}

	g_critical("%s: field-type %d isn't known yet", 
			G_STRLOC,
			type);

	return unknown_type;
}

void network_mysqld_table_print(network_mysqld_table *tbl) {
	GString *out = g_string_new(NULL);
	guint i;

	g_string_append_printf(out, "CREATE TABLE %s.%s (\n",
			tbl->db_name->str,
			tbl->table_name->str);

	for (i = 0; i < tbl->fields->len; i++) {
		MYSQL_FIELD *field = tbl->fields->pdata[i];

		if (i > 0) {
			g_string_append(out, ",\n");
		}

		switch ((guchar)field->type) {
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_YEAR:

		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			g_string_append_printf(out, "  field_%d %s %s NULL",
					i,
					network_mysqld_proto_field_get_typestring(field->type),
					field->flags & NOT_NULL_FLAG ? "NOT" : "DEFAULT"
				 );
			break;
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			g_string_append_printf(out, "  field_%d %s(%lu, %u) %s NULL",
					i,
					network_mysqld_proto_field_get_typestring(field->type),
					field->max_length, field->decimals,
					field->flags & NOT_NULL_FLAG ? "NOT" : "DEFAULT"
				 );
			break;
		default:
			g_string_append_printf(out, "  field_%d %s(%lu) %s NULL",
					i,
					network_mysqld_proto_field_get_typestring(field->type),
					field->max_length,
					field->flags & NOT_NULL_FLAG ? "NOT" : "DEFAULT"
				 );
			break;
		}
	}
	g_string_append(out, "\n)");

	g_print("-- %s:\n%s\n\n",
			G_STRLOC, out->str);

	g_string_free(out, TRUE);
}

static int network_mysqld_proto_field_append_to_string(GString *out, network_mysqld_proto_field *field) {
	switch((guchar)field->fielddef->type) {
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_TIMESTAMP:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_YEAR:

	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_LONGLONG:
	case MYSQL_TYPE_ENUM:
	case MYSQL_TYPE_SET:
		g_string_append_printf(out, "%"G_GUINT64_FORMAT, field->data.i);
		break;
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
		g_string_append_printf(out, "'%s'", field->data.s ? field->data.s : "");
		break;
	case MYSQL_TYPE_FLOAT:
	case MYSQL_TYPE_DOUBLE:
		g_string_append_printf(out, "%f", field->data.d);
		break;
	case MYSQL_TYPE_GEOMETRY:
		g_string_append_printf(out, "'...(geometry)'");
		break;
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
		g_string_append_printf(out, "'...(blob)'");
		break;
	case MYSQL_TYPE_BIT:
		g_string_append_printf(out, "'...(bit)'");
		break;
	case MYSQL_TYPE_NEWDECIMAL:
		g_string_append_printf(out, "'...(decimal)'");
		break;
	default:
		g_error("%s: field-type '%s' (%d) isn't known",
				G_STRLOC,
				network_mysqld_proto_field_get_typestring(field->fielddef->type),
				field->fielddef->type);
		break;
	}

	return 0;
}

/**
 * decode a binlog event
 */
int network_mysqld_binlog_event_print(network_mysqld_binlog *binlog, 
		network_mysqld_binlog_event *event) {
	guint i, field_ndx;
	network_mysqld_table *tbl;
	int err = 0;
#if 0
	g_message("%s: timestamp = %u, type = %u, server-id = %u, size = %u, pos = %u, flags = %04x",
			G_STRLOC,
			event->timestamp,
			event->event_type,
			event->server_id,
			event->event_size,
			event->log_pos,
			event->flags);
#endif

	switch (event->event_type) {
	case QUERY_EVENT: /* 2 */
#if 0
		g_message("%s: QUERY: thread_id = %d, exec_time = %d, error-code = %d\ndb = %s, query = %s",
				G_STRLOC,
				event->event.query_event.thread_id,
				event->event.query_event.exec_time,
				event->event.query_event.error_code,
				event->event.query_event.db_name ? event->event.query_event.db_name : "(null)",
				event->event.query_event.query ? event->event.query_event.query : "(null)"
			 );
#else
		g_print("-- %s: db = %s\n%s\n\n",
				G_STRLOC,
				event->event.query_event.db_name ? event->event.query_event.db_name : "(null)",
				event->event.query_event.query ? event->event.query_event.query : "(null)"
			 );

#endif
		break;
	case STOP_EVENT:
		break;
	case TABLE_MAP_EVENT:
		tbl = network_mysqld_table_new();

		network_mysqld_binlog_event_tablemap_get(event, tbl);
	
		g_hash_table_insert(binlog->rbr_tables, guint64_new(tbl->table_id), tbl);

		network_mysqld_table_print(tbl);
		break; 
	case FORMAT_DESCRIPTION_EVENT: /* 15 */
		g_print("-- format-description:\n");
		g_print("--   file-version: %d\n", event->event.format_event.binlog_version);
		g_print("--   writer-version: %s\n", event->event.format_event.master_version);
		g_print("--   created: %d\n", event->event.format_event.created_ts);
		g_print("--   no. of known events: %"G_GSIZE_FORMAT"\n", event->event.format_event.perm_events_len);
		break;
	case INTVAR_EVENT: /* 5 */
	 	break;
	case XID_EVENT: /* 16 */
		g_print("COMMIT /* xid = %"G_GUINT64_FORMAT" */\n", event->event.xid.xid_id);
		break;
	case ROTATE_EVENT: /* 4 */
		g_print("-- rotating to %s, pos %u\n", event->event.rotate_event.binlog_file,  event->event.rotate_event.binlog_pos);
		break;
	case WRITE_ROWS_EVENT:
	case UPDATE_ROWS_EVENT:
	case DELETE_ROWS_EVENT: {
		network_packet row_packet;
		GString row;
		tbl = g_hash_table_lookup(binlog->rbr_tables, &(event->event.row_event.table_id));

		if (!tbl) {
			g_critical("%s: table-id: %"G_GUINT64_FORMAT" isn't known, needed for a %d event",
					G_STRLOC,
					event->event.row_event.table_id,
					event->event_type
					);
			break;
		}

		row.str = event->event.row_event.row;
		row.len = event->event.row_event.row_len;

		row_packet.data = &row;
		row_packet.offset = 0;
#if 0
		g_debug_hexdump(G_STRLOC " (used colums)", event->event.row_event.used_columns, event->event.row_event.used_columns_len);
#endif

		do {
			GPtrArray *pre_fields, *post_fields = NULL;
			GString *out = g_string_new(NULL);
			gchar *post_bits = NULL, *pre_bits;

			err = err || network_mysqld_proto_get_string_len(
					&row_packet, 
					&pre_bits,
					event->event.row_event.null_bits_before_len);

			if (err) break;

			pre_fields = network_mysqld_proto_fields_new_full(tbl->fields,
					event->event.row_event.used_columns_before,
					event->event.row_event.used_columns_before_len,
					pre_bits, 
					event->event.row_event.null_bits_before_len);

			if (NULL == pre_fields) {
				err = 1;
				break;
			}

			if (network_mysqld_proto_fields_get(&row_packet, pre_fields)) {
				break;
			}

			if (event->event_type == UPDATE_ROWS_EVENT) {
				err = err || network_mysqld_proto_get_string_len(
						&row_packet, 
						&post_bits,
						event->event.row_event.null_bits_after_len);

				if (err) break;
		
				post_fields = network_mysqld_proto_fields_new_full(tbl->fields, 
					event->event.row_event.used_columns_after,
					event->event.row_event.used_columns_after_len,
					post_bits, 
					event->event.row_event.null_bits_after_len);
				if (NULL == post_fields) {
					err = 1;
					break;
				}
				if (network_mysqld_proto_fields_get(&row_packet, post_fields)) {
					break;
				}
			}

			/* call lua */

			switch (event->event_type) {
			case UPDATE_ROWS_EVENT:
				g_string_append_printf(out, "UPDATE %s.%s\n   SET ",
						tbl->db_name->str,
						tbl->table_name->str);

				for (i = 0, field_ndx = 0; i < tbl->fields->len; i++) {
					guint col_byteoffset = i / 8;
					guint col_bitoffset = i % 8;

					if ((event->event.row_event.used_columns_after[col_byteoffset] >> col_bitoffset) & 0x1) {
						network_mysqld_proto_field *field = post_fields->pdata[field_ndx++];

						if (field_ndx > 1) {
							g_string_append_printf(out, ", ");
						}
						g_string_append_printf(out, "field_%d ", i);

						if (field->is_null) {
							g_string_append(out, "= NULL");
						} else {
							g_string_append(out, "= ");
							network_mysqld_proto_field_append_to_string(out, field);
						}
					}
				}

				g_string_append_printf(out, "\n WHERE ");

				for (i = 0, field_ndx = 0; i < tbl->fields->len; i++) {
					guint col_byteoffset = i / 8;
					guint col_bitoffset = i % 8;

					if ((event->event.row_event.used_columns_before[col_byteoffset] >> col_bitoffset) & 0x1) {
						network_mysqld_proto_field *field = pre_fields->pdata[field_ndx++];
						if (field_ndx > 1) {
							g_string_append_printf(out, " AND ");
						}

						g_string_append_printf(out, "field_%d ", i);
						if (field->is_null) {
							g_string_append(out, "IS NULL");
						} else {
							g_string_append(out, "= ");
							network_mysqld_proto_field_append_to_string(out, field);
						}
					}
				}
				break;
			case WRITE_ROWS_EVENT:
				g_string_append_printf(out, "INSERT INTO %s.%s ",
						tbl->db_name->str,
						tbl->table_name->str);

				/* ... get the column-index right */
				g_string_append(out, "(");
				for (i = 0; i < tbl->fields->len; i++) {
					guint col_byteoffset = i / 8;
					guint col_bitoffset = i % 8;

					if ((event->event.row_event.used_columns_before[col_byteoffset] >> col_bitoffset) & 0x1) {
						if (out->str[out->len - 1] != '(') g_string_append(out, ", ");

						g_string_append_printf(out, "field_%d", i);
					}
				}
				g_string_append(out, ")");

				g_string_append(out, " VALUES\n  (");

				for (i = 0; i < pre_fields->len; i++) {
					network_mysqld_proto_field *field = pre_fields->pdata[i];
					if (i > 0) {
						g_string_append_printf(out, ", ");
					}
					if (field->is_null) {
						g_string_append(out, "NULL");
					} else {
						network_mysqld_proto_field_append_to_string(out, field);
					}
				}

				g_string_append_printf(out, ")");
				break;
			case DELETE_ROWS_EVENT:
				g_string_append_printf(out, "DELETE FROM %s.%s\n WHERE ",
						tbl->db_name->str,
						tbl->table_name->str);

				for (i = 0, field_ndx = 0; i < tbl->fields->len; i++) {
					guint col_byteoffset = i / 8;
					guint col_bitoffset = i % 8;

					if ((event->event.row_event.used_columns_before[col_byteoffset] >> col_bitoffset) & 0x1) {
						network_mysqld_proto_field *field = pre_fields->pdata[field_ndx++];
						if (field_ndx > 1) {
							g_string_append_printf(out, " AND ");
						}
						g_string_append_printf(out, "field_%d ", i);
						if (field->is_null) {
							g_string_append(out, "IS NULL");
						} else {
							g_string_append(out, "= ");
							network_mysqld_proto_field_append_to_string(out, field);
						}
					}
				}
				break;

			default:
				break;
			}

			g_print("%s\n\n", out->str);

			g_string_free(out, TRUE);

			if (pre_fields) network_mysqld_proto_fields_free(pre_fields);
			if (post_fields) network_mysqld_proto_fields_free(post_fields);
			if (pre_bits) g_free(pre_bits);
			if (post_bits) g_free(post_bits);
		} while (row_packet.data->len > row_packet.offset);

		if (0 == err) {
			if (row_packet.offset != row_packet.data->len) {
				g_debug("%s: event_type %d: offset = %d, length = %"G_GSIZE_FORMAT,
						G_STRLOC,
						event->event_type,
						row_packet.offset,
						row_packet.data->len);
			}
		}

		break; }
	case ROWS_QUERY_LOG_EVENT:
		g_print("-- next RBR query: %s\n", event->event.rows_query.query);
		break;

	default:
		g_message("%s: unknown event-type: %d",
				G_STRLOC,
				event->event_type);
		return -1;
	}
	return err ? -1 : 0;
}

/**
 * read a binlog file
 */
int replicate_binlog_dump_file(
		const char *filename, 
		gint startpos,
		gboolean find_startpos,
		gint stoppos
		) {
	int fd;
	char binlog_header[4];
	network_packet *packet;
	network_mysqld_binlog *binlog;
	network_mysqld_binlog_event *event;
	off_t binlog_pos;
	int round = 0;
	int ret = 0;

	if (-1 == (fd = g_open(filename, O_RDONLY, 0))) {
		g_critical("%s: opening '%s' failed: %s",
				G_STRLOC,
				filename,
				g_strerror(errno));
		return -1;
	}

	if (4 != read(fd, binlog_header, 4)) {
		g_return_val_if_reached(-1);
	}

	if (binlog_header[0] != '\xfe' ||
	    binlog_header[1] != 'b' ||
	    binlog_header[2] != 'i' ||
	    binlog_header[3] != 'n') {

		g_critical("%s: binlog-header should be: %02x%02x%02x%02x, got %02x%02x%02x%02x",
				G_STRLOC,
				'\xfe', 'b', 'i', 'n',
				binlog_header[0],
				binlog_header[1],
				binlog_header[2],
				binlog_header[3]
				);

		g_return_val_if_reached(-1);
	}

	packet = network_packet_new();
	packet->data = g_string_new(NULL);
	g_string_set_size(packet->data, 19 + 1);

	binlog = network_mysqld_binlog_new();
	binlog_pos = 4;

	if (startpos) {
		if (-1 == lseek(fd, startpos, SEEK_SET)) {
			g_critical("%s: lseek(%d) failed: %s", 
					G_STRLOC,
					startpos,
					g_strerror(errno)
					);
			g_return_val_if_reached(-1);
		}

		binlog_pos = startpos;
	}

	if (find_startpos) {
		/* check if the current binlog-pos is valid,
		 *
		 * if not, just skip a byte a retry until we found a valid header
		 * */
		while (19 == (packet->data->len = read(fd, packet->data->str, 19))) {
			packet->data->str[packet->data->len] = '\0'; /* term the string */
			packet->offset = 0;

			g_assert_cmpint(packet->data->len, ==, 19);

			event = network_mysqld_binlog_event_new();
			network_mysqld_proto_get_binlog_event_header(packet, event);

			if (event->event_size < 19 ||
			    binlog_pos + event->event_size != event->log_pos) {
				if (-1 == lseek(fd, -18, SEEK_CUR)) {
					g_critical("%s: lseek(%d) failed: %s", 
							G_STRLOC,
							-18,
							g_strerror(errno)
							);
					g_return_val_if_reached(-1);
				}

				binlog_pos += 1;

				g_message("%s: --binlog-start-pos isn't valid, trying to sync at %ld (attempt: %d)", 
						G_STRLOC,
						binlog_pos,
						round++
						);
			} else {
				if (-1 == lseek(fd, -19, SEEK_CUR)) {
					g_critical("%s: lseek(%d) failed: %s", 
							G_STRLOC,
							-18,
							g_strerror(errno)
							);
					g_return_val_if_reached(-1);
				}

				network_mysqld_binlog_event_free(event);
				
				break;
			}
			network_mysqld_binlog_event_free(event);
		}
	} 

	packet->offset = 0;

	/* next are the events, without the mysql packet header */
	while (19 == (packet->data->len = read(fd, packet->data->str, 19)) && (stoppos <= 0 || binlog_pos < stoppos)) {
		gssize len;
		packet->data->str[packet->data->len] = '\0'; /* term the string */

		g_assert_cmpint(packet->data->len, ==, 19);
	
		event = network_mysqld_binlog_event_new();
		network_mysqld_proto_get_binlog_event_header(packet, event);

		if (event->event_size < 19 ||
		    binlog_pos + event->event_size != event->log_pos) {
			g_critical("%s: binlog-pos=%ld is invalid, you may want to start with --binlog-find-start-pos",
				G_STRLOC,
				binlog_pos
			       );
			ret = -1;
			break;
		}

		g_print("-- (--binlog-start-pos=%ld (next event at %"G_GUINT32_FORMAT")) event = %s (%d)\n",
				binlog_pos,
				event->log_pos,
				network_mysqld_binlog_get_eventname(event->event_type),
				event->event_type
				);
	
		binlog_pos += 19;

		g_string_set_size(packet->data, event->event_size); /* resize the string */
		packet->data->len = 19;

		len = read(fd, packet->data->str + 19, event->event_size - 19);

		if (-1 == len) {
			g_critical("%s: lseek(..., %d, ...) failed: %s",
					G_STRLOC,
					event->event_size - 19,
					g_strerror(errno));
			return -1;
		}
		g_assert_cmpint(len, ==, event->event_size - 19); /* read error */

		g_assert_cmpint(packet->data->len, ==, 19);
		packet->data->len += len;
		g_assert_cmpint(packet->data->len, ==, event->event_size);
		
		if (network_mysqld_proto_get_binlog_event(packet, binlog, event)) {
			g_debug_hexdump(G_STRLOC, packet->data->str + 19, packet->data->len - 19);
		} else if (network_mysqld_binlog_event_print(binlog, event)) {
			g_debug_hexdump(G_STRLOC, packet->data->str + 19, packet->data->len - 19);
			/* ignore it */
		}
	
		network_mysqld_binlog_event_free(event);

		packet->offset = 0;
		binlog_pos += len;
	}
	g_string_free(packet->data, TRUE);
	network_packet_free(packet);

	network_mysqld_binlog_free(binlog);

	close(fd);

	return ret;
}

#define GETTEXT_PACKAGE "mysql-binlog-dump"

int main(int argc, char **argv) {
	chassis *chas;
	
	/* read the command-line options */
	GOptionContext *option_ctx;
	GError *gerr = NULL;
	guint i;
	int exit_code = EXIT_SUCCESS;
	int print_version = 0;
	const gchar *check_str = NULL;
	gchar *default_file = NULL;

	gchar *log_level = NULL;
	gchar *binlog_filename = NULL;

	GKeyFile *keyfile = NULL;
	chassis_log *log;
	gint binlog_start_pos = 0;
	gint binlog_stop_pos = 0;
	gboolean binlog_find_start_pos = FALSE;

	/* can't appear in the configfile */
	GOptionEntry base_main_entries[] = 
	{
		{ "version",                 'V', 0, G_OPTION_ARG_NONE, NULL, "Show version", NULL },
		{ "defaults-file",            0, 0, G_OPTION_ARG_STRING, NULL, "configuration file", "<file>" },
		
		{ NULL,                       0, 0, G_OPTION_ARG_NONE,   NULL, NULL, NULL }
	};

	GOptionEntry main_entries[] = 
	{
		{ "log-level",                0, 0, G_OPTION_ARG_STRING, NULL, "log all messages of level ... or higer", "(error|warning|info|message|debug)" },
		{ "log-file",                 0, 0, G_OPTION_ARG_STRING, NULL, "log all messages in a file", "<file>" },
		{ "log-use-syslog",           0, 0, G_OPTION_ARG_NONE, NULL, "send all log-messages to syslog", NULL },
		
		{ "binlog-file",              0, 0, G_OPTION_ARG_FILENAME, NULL, "binlog filename", NULL },
		{ "binlog-start-pos",         0, 0, G_OPTION_ARG_INT, NULL, "binlog start position", NULL },
		{ "binlog-stop-pos",          0, 0, G_OPTION_ARG_INT, NULL, "binlog stop position", NULL },
		{ "binlog-find-start-pos",    0, 0, G_OPTION_ARG_NONE, NULL, "find binlog start position", NULL },
		
		{ NULL,                       0, 0, G_OPTION_ARG_NONE,   NULL, NULL, NULL }
	};

	if (!GLIB_CHECK_VERSION(2, 6, 0)) {
		g_error("the glib header are too old, need at least 2.6.0, got: %d.%d.%d", 
				GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);
	}

	check_str = glib_check_version(GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);

	if (check_str) {
		g_error("%s, got: lib=%d.%d.%d, headers=%d.%d.%d", 
			check_str,
			glib_major_version, glib_minor_version, glib_micro_version,
			GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);
	}

#if defined(HAVE_LUA_H)
# if defined(DATADIR)
	/**
	 * if the LUA_PATH or LUA_CPATH are not set, set a good default 
	 */
	if (!g_getenv(LUA_PATH)) {
		g_setenv(LUA_PATH, 
				DATADIR "/?.lua", 1);
	}
# endif

# if defined(LIBDIR)
	if (!g_getenv(LUA_CPATH)) {
#  if _WIN32
		g_setenv(LUA_CPATH, 
				LIBDIR "/?.dll", 1);
#  else
		g_setenv(LUA_CPATH, 
				LIBDIR "/?.so", 1);
#  endif
	}
# endif
#endif

	g_thread_init(NULL);

	log = chassis_log_new();
	log->min_lvl = G_LOG_LEVEL_MESSAGE; /* display messages while parsing or loading plugins */
	
	g_log_set_default_handler(chassis_log_func, log);

	chas = chassis_new();

	i = 0;
	base_main_entries[i++].arg_data  = &(print_version);
	base_main_entries[i++].arg_data  = &(default_file);

	i = 0;
	main_entries[i++].arg_data  = &(log_level);
	main_entries[i++].arg_data  = &(log->log_filename);
	main_entries[i++].arg_data  = &(log->use_syslog);
	main_entries[i++].arg_data  = &(binlog_filename);
	main_entries[i++].arg_data  = &(binlog_start_pos);
	main_entries[i++].arg_data  = &(binlog_stop_pos);
	main_entries[i++].arg_data  = &(binlog_find_start_pos);

	option_ctx = g_option_context_new("- MySQL Binlog Dump");
	g_option_context_add_main_entries(option_ctx, base_main_entries, GETTEXT_PACKAGE);
	g_option_context_add_main_entries(option_ctx, main_entries, GETTEXT_PACKAGE);
	g_option_context_set_help_enabled(option_ctx, TRUE);

	/**
	 * parse once to get the basic options like --defaults-file and --version
	 *
	 * leave the unknown options in the list
	 */
	if (FALSE == g_option_context_parse(option_ctx, &argc, &argv, &gerr)) {
		g_critical("%s", gerr->message);
		
		exit_code = EXIT_FAILURE;
		goto exit_nicely;
	}

	if (default_file) {
		keyfile = g_key_file_new();
		g_key_file_set_list_separator(keyfile, ',');

		if (FALSE == g_key_file_load_from_file(keyfile, default_file, G_KEY_FILE_NONE, &gerr)) {
			g_critical("loading configuration from %s failed: %s", 
					default_file,
					gerr->message);

			exit_code = EXIT_FAILURE;
			goto exit_nicely;
		}
	}

	if (print_version) {
		printf("%s\r\n", PACKAGE_STRING); 
		printf("  glib2: %d.%d.%d\r\n", GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);

		exit_code = EXIT_SUCCESS;
		goto exit_nicely;
	}


	/* add the other options which can also appear in the configfile */
	g_option_context_add_main_entries(option_ctx, main_entries, GETTEXT_PACKAGE);

	/**
	 * parse once to get the basic options 
	 *
	 * leave the unknown options in the list
	 */
	if (FALSE == g_option_context_parse(option_ctx, &argc, &argv, &gerr)) {
		g_critical("%s", gerr->message);

		exit_code = EXIT_FAILURE;
		goto exit_nicely;
	}

	if (keyfile) {
		if (chassis_keyfile_to_options(keyfile, "mysql-binlog-dump", main_entries)) {
			exit_code = EXIT_FAILURE;
			goto exit_nicely;
		}
	}

	if (log->log_filename) {
		if (0 == chassis_log_open(log)) {
			g_critical("can't open log-file '%s': %s", log->log_filename, g_strerror(errno));

			exit_code = EXIT_FAILURE;
			goto exit_nicely;
		}
	}


	/* handle log-level after the config-file is read, just in case it is specified in the file */
	if (log_level) {
		if (0 != chassis_log_set_level(log, log_level)) {
			g_critical("--log-level=... failed, level '%s' is unknown ", log_level);

			exit_code = EXIT_FAILURE;
			goto exit_nicely;
		}
	} else {
		/* if it is not set, use "critical" as default */
		log->min_lvl = G_LOG_LEVEL_CRITICAL;
	}

	if (!binlog_filename) {
		exit_code = EXIT_FAILURE;
		goto exit_nicely;
	}

	replicate_binlog_dump_file(
			binlog_filename,
			binlog_start_pos,
			binlog_find_start_pos,
			binlog_stop_pos
			);

exit_nicely:
	if (option_ctx) g_option_context_free(option_ctx);
	if (keyfile) g_key_file_free(keyfile);
	if (default_file) g_free(default_file);
	if (binlog_filename) g_free(binlog_filename);
	if (gerr) g_error_free(gerr);

	if (log_level) g_free(log_level);
	if (chas) chassis_free(chas);
	
	chassis_log_free(log);

	return exit_code;
}


