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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include <stdio.h>

/**
 * replication 
 */
#include "glib-ext.h"
#include "network-mysqld-binlog.h"

#define S(x) x->str, x->len

network_mysqld_table *network_mysqld_table_new() {
	network_mysqld_table *tbl;

	tbl = g_new0(network_mysqld_table, 1);
	tbl->db_name = g_string_new(NULL);
	tbl->table_name = g_string_new(NULL);

	tbl->fields = network_mysqld_proto_fielddefs_new();

	return tbl;
}

void network_mysqld_table_free(network_mysqld_table *tbl) {
	if (!tbl) return;

	g_string_free(tbl->db_name, TRUE);
	g_string_free(tbl->table_name, TRUE);

	network_mysqld_proto_fielddefs_free(tbl->fields);

	g_free(tbl);
}

static guint guint64_hash(gconstpointer _key) {
	const guint64 *key = _key;

	return *key & 0xffffffff;
}

static gboolean guint64_equal(gconstpointer _a, gconstpointer _b) {
	const guint64 *a = _a;
	const guint64 *b = _b;

	return *a == *b;
}

guint64 *guint64_new(guint64 i) {
	guint64 *ip;

	ip = g_new0(guint64, 1);
	*ip = i;

	return ip;
}

network_mysqld_binlog *network_mysqld_binlog_new() {
	network_mysqld_binlog *binlog;

	binlog = g_new0(network_mysqld_binlog, 1);
	binlog->rbr_tables = g_hash_table_new_full(
			guint64_hash,
			guint64_equal,
			g_free,
			(GDestroyNotify)network_mysqld_table_free);

	return binlog;
}

void network_mysqld_binlog_free(network_mysqld_binlog *binlog) {
	if (!binlog) return;

	g_hash_table_destroy(binlog->rbr_tables);

	g_free(binlog);
}

network_mysqld_binlog_event *network_mysqld_binlog_event_new() {
	network_mysqld_binlog_event *binlog;

	binlog = g_new0(network_mysqld_binlog_event, 1);

	return binlog;
}

int network_mysqld_proto_get_binlog_status(network_packet *packet) {
	guint8 ok;

	/* on the network we have a length and packet-number of 4 bytes */
	if (0 != network_mysqld_proto_get_int8(packet, &ok)) {
		return -1;
	}
	g_return_val_if_fail(ok == 0, -1);

	return 0;
}

int network_mysqld_proto_get_binlog_event_header(network_packet *packet, network_mysqld_binlog_event *event) {
	int err = 0;
	guint8 event_type;

	err = err || network_mysqld_proto_get_int32(packet, &event->timestamp);
	err = err || network_mysqld_proto_get_int8(packet,  &event_type);
	err = err || network_mysqld_proto_get_int32(packet, &event->server_id);
	err = err || network_mysqld_proto_get_int32(packet, &event->event_size);
	err = err || network_mysqld_proto_get_int32(packet, &event->log_pos);
	err = err || network_mysqld_proto_get_int16(packet, &event->flags);

	if (!err) {
	       	/* event->event_type is a enum, network_mysqld_proto_get_int8() wants a guint8 ... on x86 a passing the &(enum) down works, on sparc it doesn't */

		event->event_type = event_type;
	}

	return err ? -1 : 0;
}

int network_mysqld_proto_get_binlog_event(network_packet *packet, 
		network_mysqld_binlog *binlog,
		network_mysqld_binlog_event *event) {

	int err = 0;
	int maj, min, pat;

	if (binlog->checksum == NETWORK_MYSQLD_BINLOG_CHECKSUM_CRC32) {
		/* patch the packet-len for the decoders as if there would be no checksum */
		packet->data->len -= 4;
	}

	switch ((guchar)event->event_type) {
	case QUERY_EVENT:
		err = err || network_mysqld_proto_get_int32(packet, &event->event.query_event.thread_id);
		err = err || network_mysqld_proto_get_int32(packet, &event->event.query_event.exec_time);
		err = err || network_mysqld_proto_get_int8(packet, &event->event.query_event.db_name_len);
		err = err || network_mysqld_proto_get_int16(packet, &event->event.query_event.error_code);

		/* 5.0 has more flags */
		if (packet->data->len > packet->offset) {
			guint16 var_size = 0;

			err = err || network_mysqld_proto_get_int16(packet, &var_size);
			if (var_size) {
				/* skip the variable size part for now */
				err = err || network_mysqld_proto_skip(packet, var_size);
			}
	
			/* default db has <db_name_len> chars */
			err = err || network_mysqld_proto_get_string_len(packet, 
					&event->event.query_event.db_name,
					event->event.query_event.db_name_len);
			err = err || network_mysqld_proto_skip(packet, 1); /* the \0 */
	
			err = err || network_mysqld_proto_get_string_len(packet, 
					&event->event.query_event.query,
					packet->data->len - packet->offset);
		}

		break;
	case ROTATE_EVENT:
		err = err || network_mysqld_proto_get_int32(packet, &event->event.rotate_event.binlog_pos);
		err = err || network_mysqld_proto_skip(packet, 4);
		err = err || network_mysqld_proto_get_string_len(
				packet, 
				&event->event.rotate_event.binlog_file,
				packet->data->len - packet->offset);
		break;
	case STOP_EVENT:
		/* is empty */
		break;
	case FORMAT_DESCRIPTION_EVENT:
		err = err || network_mysqld_proto_get_int16(packet, &event->event.format_event.binlog_version);
		err = err || network_mysqld_proto_get_string_len( /* NUL-term string */
				packet, 
				&event->event.format_event.master_version,
				ST_SERVER_VER_LEN);
		err = err || network_mysqld_proto_get_int32(packet, 
				&event->event.format_event.created_ts);

		/* the header length may change in the future, for now we assume it is 19 */
		err = err || network_mysqld_proto_get_int8(packet, &event->event.format_event.log_header_len);
		g_assert_cmpint(event->event.format_event.log_header_len, ==, 19);

		/* decode the server-version string into a integer */
		err = err || (3 != sscanf(event->event.format_event.master_version, "%d.%d.%d%*s", &maj, &min, &pat));
		err = err || (maj > 100 || maj < 0);
		err = err || (min > 100 || min < 0);
		err = err || (pat > 100 || pat < 0);

		if ((maj << 16) + (min << 8) + (pat << 0) >= 0x050602) {
			event->event.format_event.perm_events_len = packet->data->len - packet->offset;
			g_assert_cmpint(event->event.format_event.perm_events_len, ==, ENUM_END_EVENT - 1 + 1 + 4);

			event->event.format_event.perm_events_len -= 1 + 4; /* FIXME: check how it is implemented without CRC32 */
		} else {
			event->event.format_event.perm_events_len = packet->data->len - packet->offset;
		}
		/* event-length, one byte for each event */
		err = err || network_mysqld_proto_get_string_len(
				packet, 
				&event->event.format_event.perm_events,
				event->event.format_event.perm_events_len);

		/* check if we have checksums */
		if ((maj << 16) + (min << 8) + (pat << 0) >= 0x050602) {
			guint8 checksum;

			g_assert_cmpint(packet->data->len - packet->offset, ==, 1 + 4);

			err = err || network_mysqld_proto_get_int8(packet, &checksum);

			binlog->checksum = checksum;

			/* leave the last 4 bytes (CRC32 checksum) in the buffer */
		}

		if (binlog->checksum == NETWORK_MYSQLD_BINLOG_CHECKSUM_CRC32) {
			/* patch the packet len to only cover the event without the CRC32 checksum */
			packet->data->len -= 4;
		}
		
		break;
	case USER_VAR_EVENT:
		err = err || network_mysqld_proto_get_int32(
				packet,
				&event->event.user_var_event.name_len);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.user_var_event.name,
				event->event.user_var_event.name_len);

		err = err || network_mysqld_proto_get_int8(packet,
				&event->event.user_var_event.is_null);
		err = err || network_mysqld_proto_get_int8(packet,
				&event->event.user_var_event.type );
		err = err || network_mysqld_proto_get_int32(packet,
				&event->event.user_var_event.charset);
		err = err || network_mysqld_proto_get_int32(packet,
				&event->event.user_var_event.value_len);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.user_var_event.value,
				event->event.user_var_event.value_len);
		break;
	case TABLE_MAP_EVENT: /* 19 */
		/**
		 * looks like a abstract definition of a table 
		 *
		 * no, we don't want to know
		 */
		err = err || network_mysqld_proto_get_int48(packet,
				&event->event.table_map_event.table_id); /* 6 bytes */
		err = err || network_mysqld_proto_get_int16(packet,
				&event->event.table_map_event.flags);

		err = err || network_mysqld_proto_get_int8(packet,
				&event->event.table_map_event.db_name_len);
		
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.table_map_event.db_name,
				event->event.table_map_event.db_name_len);
		err = err || network_mysqld_proto_skip(packet, 1); /* this should be NUL */

		err = err || network_mysqld_proto_get_int8(packet,
				&event->event.table_map_event.table_name_len);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.table_map_event.table_name,
				event->event.table_map_event.table_name_len);
		err = err || network_mysqld_proto_skip(packet, 1); /* this should be NUL */

		err = err || network_mysqld_proto_get_lenenc_int(packet,
				&event->event.table_map_event.columns_len);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.table_map_event.columns,
				event->event.table_map_event.columns_len);

		err = err || network_mysqld_proto_get_lenenc_int(packet,
				&event->event.table_map_event.metadata_len);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.table_map_event.metadata,
				event->event.table_map_event.metadata_len);

		/**
		 * the null-bit count is columns/8 
		 */

		if (!err) event->event.table_map_event.null_bits_len = (int)((event->event.table_map_event.columns_len+7)/8);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.table_map_event.null_bits,
				event->event.table_map_event.null_bits_len);

		if (packet->data->len != packet->offset) { /* this should be the full packet */
			g_critical("%s: TABLE_MAP_EVENT at pos %u we still have %"G_GSIZE_FORMAT" left", 
					G_STRLOC,
					packet->offset,
					packet->data->len - packet->offset);
			err = 1;
		}
		break;
	case DELETE_ROWS_EVENT: /* 25 */
	case UPDATE_ROWS_EVENT: /* 24 */
	case WRITE_ROWS_EVENT: { /* 23 */
		guint col, cols_before_present, cols_after_present;

		err = err || network_mysqld_proto_get_int48(packet,
				&event->event.row_event.table_id); /* 6 bytes */
		err = err || network_mysqld_proto_get_int16(packet,
				&event->event.row_event.flags );
		
		err = err || network_mysqld_proto_get_lenenc_int(packet,
				&event->event.row_event.columns_len);

		/* a bit-mask of used-fields (m_cols.bitmap) */
		event->event.row_event.used_columns_before_len = (int)((event->event.row_event.columns_len+7)/8);
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.row_event.used_columns_before,
				event->event.row_event.used_columns_before_len);

		if (event->event_type == UPDATE_ROWS_EVENT) {
			/* used columns in the after image */
			event->event.row_event.used_columns_after_len = (int)((event->event.row_event.columns_len+7)/8);
			err = err || network_mysqld_proto_get_string_len(
					packet,
					&event->event.row_event.used_columns_after,
					event->event.row_event.used_columns_after_len);
		}

		/* we only have as many NULL-bits as we have columns that can be nullable */
		for (col = 0, cols_before_present = 0, cols_after_present = 0;
				col < event->event.row_event.columns_len;
				col++) {
			int byte_ndx = col / 8;
			int bit_ndx  = col % 8;

			if ((event->event.row_event.used_columns_before[byte_ndx] >> bit_ndx) & 1) {
				cols_before_present++;
			}
			if (event->event_type == UPDATE_ROWS_EVENT) {
				if ((event->event.row_event.used_columns_after[byte_ndx] >> bit_ndx) & 1) {
					cols_after_present++;
				}
			}
		}

		/* null-bits for all the columns */
		event->event.row_event.null_bits_before_len = (int)((cols_before_present+7)/8);
		event->event.row_event.null_bits_after_len = (int)((cols_after_present+7)/8);

		/* the null-bits + row,
		 *
		 * the rows are stored in field-format, to decode we have to see
		 * the table description
		 */
		event->event.row_event.row_len = packet->data->len - packet->offset;
		err = err || network_mysqld_proto_get_string_len(
				packet,
				&event->event.row_event.row,
				event->event.row_event.row_len);
		
		break; }
	case INTVAR_EVENT:
		err = err || network_mysqld_proto_get_int8(packet,
				&event->event.intvar.type);
		err = err || network_mysqld_proto_get_int64(packet,
				&event->event.intvar.value);

		break;
	case XID_EVENT:
		err = err || network_mysqld_proto_get_int64(packet,
				&event->event.xid.xid_id);
		break;
	case ROWS_QUERY_LOG_EVENT:
		err = err || network_mysqld_proto_get_int8(packet, &event->event.rows_query.query_len); /* a length byte */
		err = err || network_mysqld_proto_get_string_len(
				packet, 
				&event->event.rows_query.query,
				packet->data->len - packet->offset);
		break;
	default:
		g_critical("%s: unhandled binlog-event: %d", 
				G_STRLOC, 
				event->event_type);
		return -1;
	}

	if (binlog->checksum == NETWORK_MYSQLD_BINLOG_CHECKSUM_CRC32) {
		/* patch the packet-len back */
		packet->data->len += 4;

		/* the next 4 bytes are CRC32 if checksum is CRC32 */
		err = err || network_mysqld_proto_skip(packet, 4);
	}

	/* check if we have handled all bytes */
	if (packet->offset != packet->data->len) {
		g_debug("%s: event_type %d: offset = %d, length = %"G_GSIZE_FORMAT,
				G_STRLOC,
				event->event_type,
				packet->offset,
				packet->data->len);
		return -1;
	}

	return err ? -1 : 0;
}

void network_mysqld_binlog_event_free(network_mysqld_binlog_event *event) {
	if (!event) return;

	switch (event->event_type) {
	case QUERY_EVENT:
		if (event->event.query_event.db_name) g_free(event->event.query_event.db_name);
		if (event->event.query_event.query) g_free(event->event.query_event.query);
		break;
	case ROTATE_EVENT:
		if (event->event.rotate_event.binlog_file) g_free(event->event.rotate_event.binlog_file);
		break;
	case FORMAT_DESCRIPTION_EVENT:
		if (event->event.format_event.master_version) g_free(event->event.format_event.master_version);
		if (event->event.format_event.perm_events) g_free(event->event.format_event.perm_events);
		break;
	case USER_VAR_EVENT:
		if (event->event.user_var_event.name) g_free(event->event.user_var_event.name);
		if (event->event.user_var_event.value) g_free(event->event.user_var_event.value);
		break;
	case TABLE_MAP_EVENT:
		if (event->event.table_map_event.db_name) g_free(event->event.table_map_event.db_name);
		if (event->event.table_map_event.table_name) g_free(event->event.table_map_event.table_name);
		if (event->event.table_map_event.columns) g_free(event->event.table_map_event.columns);
		if (event->event.table_map_event.metadata) g_free(event->event.table_map_event.metadata);
		if (event->event.table_map_event.null_bits) g_free(event->event.table_map_event.null_bits);
		break;
	case DELETE_ROWS_EVENT:
	case UPDATE_ROWS_EVENT:
	case WRITE_ROWS_EVENT:
		if (event->event.row_event.used_columns_before) g_free(event->event.row_event.used_columns_before);
		if (event->event.row_event.used_columns_after) g_free(event->event.row_event.used_columns_after);
		if (event->event.row_event.row) g_free(event->event.row_event.row);
		break;
	case ROWS_QUERY_LOG_EVENT:
		if (event->event.rows_query.query) g_free(event->event.rows_query.query);
		break;
	default:
		break;
	}

	g_free(event);
}


network_mysqld_binlog_dump *network_mysqld_binlog_dump_new() {
	network_mysqld_binlog_dump *dump;

	dump = g_new0(network_mysqld_binlog_dump, 1);

	return dump;
}

int network_mysqld_proto_append_binlog_dump(GString *packet, network_mysqld_binlog_dump *dump) {
	network_mysqld_proto_append_int8(packet, COM_BINLOG_DUMP);
	network_mysqld_proto_append_int32(packet, dump->binlog_pos);
	network_mysqld_proto_append_int16(packet, dump->flags); /* flags */
	network_mysqld_proto_append_int32(packet, dump->server_id);
	g_string_append(packet, dump->binlog_file); /* filename */
	network_mysqld_proto_append_int8(packet, 0); /* term-nul */

	return 0;
}

void network_mysqld_binlog_dump_free(network_mysqld_binlog_dump *dump) {
	if (!dump) return;

	g_free(dump);
}


/**
 * decode the table-map event
 *
 * 
 */
int network_mysqld_binlog_event_tablemap_get(
		network_mysqld_binlog_event *event,
		network_mysqld_table *tbl) {

	network_packet metadata_packet;
	GString row;
	guint i;
	int err = 0;

	g_string_assign(tbl->db_name, event->event.table_map_event.db_name);
	g_string_assign(tbl->table_name, event->event.table_map_event.table_name);

	tbl->table_id = event->event.table_map_event.table_id;

	row.str = event->event.table_map_event.metadata;
	row.len = event->event.table_map_event.metadata_len;

	metadata_packet.data = &row;
	metadata_packet.offset = 0;

	/* the metadata is field specific */
	for (i = 0; i < event->event.table_map_event.columns_len; i++) {
		MYSQL_FIELD *field = network_mysqld_proto_fielddef_new();
		enum enum_field_types col_type;
		guint8 byte0, byte1;
		guint16 varchar_length;

		guint byteoffset = i / 8;
		guint bitoffset = i % 8;

		field->flags |= (event->event.table_map_event.null_bits[byteoffset] >> bitoffset) & 0x1 ? 0 : NOT_NULL_FLAG;

		col_type = (enum enum_field_types)event->event.table_map_event.columns[i];

		/* the meta-data depends on the type,
		 *
		 * string has 2 byte field-length
		 * floats have precision
		 * ints have display length
		 * */
		switch ((guchar)col_type) {
		case MYSQL_TYPE_STRING: /* 254 (CHAR) */
			/**
			 * due to #37426 the encoding is a bit tricky in 5.1.26+
			 * as we need at least 10 bits to encode the max-field-length
			 *
			 * < 5.1.26
			 *
			 *   byte0      byte1
			 *   xxxx xxxx  .... ....  real-type
			 *   .... ....  xxxx xxxx  field-length
			 *   7       0  7       0
			 *
			 * >= 5.1.26
			 *   byte0      byte1
			 *   xx11 xxxx  .... ....  real-type
			 *   ..xx ....  xxxx xxxx  field-length
			 *   7       0  7       0
			 * 
			 */

			/* byte 0: real_type + upper bits of field-length (see #37426)
			 * byte 1: field-length
			 */
			err = err || network_mysqld_proto_get_int8(&metadata_packet, 
					&byte0);
			err = err || network_mysqld_proto_get_int8(&metadata_packet, 
					&byte1);
			if (!err) {
				if ((byte0 & 0x30) != 0x30) {
					/* a long CHAR() field */
					field->max_length |= (((byte0 & 0x30) ^ 0x30) << 4);
					field->type = byte0 | 0x30; /* see #37426 */
				} else {
					field->max_length = byte1;
					field->type       = byte0;
				}
			}

			break;
		case MYSQL_TYPE_VARCHAR: /* 15 (VARCHAR) */
		case MYSQL_TYPE_VAR_STRING:
			/* 2 byte length (int2store)
			 */
			err = err || network_mysqld_proto_get_int16(&metadata_packet, &varchar_length);

			if (!err) {
				field->type = col_type;
				field->max_length = varchar_length;
			}
			break;
		case MYSQL_TYPE_GEOMETRY: /* 255 */
		case MYSQL_TYPE_BLOB: /* 252 */
			/* the packlength (1 .. 4) */
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte0);

			if (!err) {
				field->type = (guchar)col_type;
				field->max_length = byte0;

				/* fix the field-type */
				if (field->type == MYSQL_TYPE_BLOB) {
					switch (field->max_length) {
					case 1:
						field->type = MYSQL_TYPE_TINY_BLOB;
						break;
					case 2:
						field->type = MYSQL_TYPE_BLOB;
						break;
					case 3:
						field->type = MYSQL_TYPE_MEDIUM_BLOB;
						break;
					case 4:
						field->type = MYSQL_TYPE_LONG_BLOB;
						break;
					default:
						err = 1;
						break;
					}
				} else if (field->type == MYSQL_TYPE_GEOMETRY) {
					if (field->max_length != 4) {
						err = 1;
					}
				} else {
					g_assert_not_reached();
				}
			}

			break;
		case MYSQL_TYPE_NEWDECIMAL:
			field->type = col_type;
			/**
			 * byte 0: precisions
			 * byte 1: decimals
			 */
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte0);
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte1);

			if (!err) {
				field->max_length = byte0;
				field->decimals   = byte1;
			}
			break;
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_FLOAT:
			/* pack-length */
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte0);

			if (!err) {
				field->type = col_type;
				field->max_length = byte0;
			}
			break;
		case MYSQL_TYPE_ENUM:
			/* real-type (ENUM|SET)
			 * pack-length
			 */
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte0);
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte1);

			if (!err) {
				field->type = byte0;
				field->max_length = byte1;
			}
			break;
		case MYSQL_TYPE_BIT:
			/*
			 * [0] - bitlength % 8
			 * [1] - bitlength / 8
			 */
			field->type = col_type;

			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte0);
			err = err || network_mysqld_proto_get_int8(&metadata_packet, &byte1);

			if (!err) {
				field->max_length = byte1 + (byte0 == 0 ? 0 : 1);
			}

			break;

		/* no metadata */
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_DATE: 
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_YEAR:

		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
			field->type = col_type;
			break;
		default:
			g_error("%s: field-type %d isn't handled",
					G_STRLOC,
					col_type
					);
			break;
		}

		g_ptr_array_add(tbl->fields, field);
	}

	if (metadata_packet.offset != metadata_packet.data->len) {
		g_debug_hexdump(G_STRLOC, event->event.table_map_event.columns, event->event.table_map_event.columns_len);
		g_debug_hexdump(G_STRLOC, event->event.table_map_event.metadata, event->event.table_map_event.metadata_len);
	}
	if (metadata_packet.offset != metadata_packet.data->len) {
		g_critical("%s: ",
				G_STRLOC);
		err = 1;
	}

	return err ? -1 : 0;
}


