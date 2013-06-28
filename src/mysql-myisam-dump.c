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

#define S(x) x->str, x->len


typedef struct {
	MYSQL_FIELD *fielddef;

	union {
		guint64 i;
		gchar *s;
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

	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:

	case MYSQL_TYPE_DECIMAL:
	case MYSQL_TYPE_NEWDECIMAL:

	case MYSQL_TYPE_ENUM:
		break;
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
		if (field->data.s) g_free(field->data.s);
		break;
	default:
		g_message("%s: unknown field_type to free: %d",
				G_STRLOC,
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

	switch ((guchar)field->fielddef->type) {
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
		err = err || network_mysqld_proto_get_int24(packet, &i32);
		if (!err) field->data.i = i32;
		break;
	case MYSQL_TYPE_SHORT:     
		err = err || network_mysqld_proto_get_int16(packet, &i16);
		if (!err) field->data.i = i16;
		break;
	case MYSQL_TYPE_TINY:     
		err = err || network_mysqld_proto_get_int8(packet, &i8);
		if (!err) field->data.i = i8;
		break;
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
	case MYSQL_TYPE_BLOB:
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
		g_error("%s: unknown field-type to fetch: %d",
				G_STRLOC,
				field->fielddef->type);
		break;
	}

	return err ? -1 : 0;
}

GPtrArray *network_mysqld_proto_fields_new_full(
		GPtrArray *fielddefs,
		gchar *null_bits,
		guint G_GNUC_UNUSED null_bits_len) {
	GPtrArray *fields;
	guint i;

	fields = g_ptr_array_new();

	for (i = 0; i < fielddefs->len; i++) {
		MYSQL_FIELD *fielddef = fielddefs->pdata[i];
		network_mysqld_proto_field *field = network_mysqld_proto_field_new();

		guint byteoffset = i / 8;
		guint bitoffset = i % 8;

		field->fielddef = fielddef;
		field->is_null = (null_bits[byteoffset] >> bitoffset) & 0x1;

		/* the field is defined as NOT NULL, so the null-bit shouldn't be set */
		if ((fielddef->flags & NOT_NULL_FLAG) != 0) {
			if (field->is_null) {
				g_error("%s: [%d] field is defined as NOT NULL, but nul-bit is set",
						G_STRLOC,
						i
						);
			}
		}
		g_ptr_array_add(fields, field);
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
	enum enum_field_types type;
	const char *name;
} field_type_name[] = {
	{ MYSQL_TYPE_STRING, "CHAR" },
	{ MYSQL_TYPE_VARCHAR, "VARCHAR" },
	{ MYSQL_TYPE_BLOB, "BLOB" },

	{ MYSQL_TYPE_TINY, "TINYINT" },
	{ MYSQL_TYPE_SHORT, "SMALLINT" },
	{ MYSQL_TYPE_INT24, "MEDIUMINT" },
	{ MYSQL_TYPE_LONG, "INT" },
	{ MYSQL_TYPE_NEWDECIMAL, "DECIMAL" },

	{ MYSQL_TYPE_ENUM, "ENUM" },

	{ MYSQL_TYPE_TIMESTAMP, "TIMESTAMP" },
	{ MYSQL_TYPE_DATE, "DATE" },
	{ MYSQL_TYPE_DATETIME, "DATETIME" },

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

typedef struct {
	guint16 fieldnr;
	guint16 offset;
	guint16 key_type;
	guint8  flags;
	guint16 length;
} network_mysqld_keypart;

network_mysqld_keypart *network_mysqld_keypart_new() {
	network_mysqld_keypart *keypart;

	keypart = g_new0(network_mysqld_keypart, 1);

	return keypart;
}

void network_mysqld_keypart_free(network_mysqld_keypart *keypart) {
	if (!keypart) return;

	g_free(keypart);
}


typedef struct {
	guint16 flags;
	guint16 key_length;
	guint8  key_parts;
	guint8  algorithm;
	guint16 block_size;

	GPtrArray *parts;

	GString *name;
} network_mysqld_keyinfo;

network_mysqld_keyinfo *network_mysqld_keyinfo_new() {
	network_mysqld_keyinfo *keyinfo;

	keyinfo = g_new0(network_mysqld_keyinfo, 1);
	keyinfo->parts = g_ptr_array_new();
	keyinfo->name  = g_string_new(NULL);

	return keyinfo;
}

void network_mysqld_keyinfo_free(network_mysqld_keyinfo *keyinfo) {
	guint i;

	if (!keyinfo) return;

	for (i = 0; i < keyinfo->parts->len; i++) {
		network_mysqld_keypart_free(keyinfo->parts->pdata[i]);
	}
	g_ptr_array_free(keyinfo->parts, TRUE);
	g_string_free(keyinfo->name, TRUE);

	g_free(keyinfo);
}

typedef struct {
	guint16 field_len;
	guint32 rec_pos;
	guint16 pack_flags;
	guint8  unireg_type;
	guint8  col_values_ndx;
	guint16 comment_len;
	guint8  field_type;
	guint8  geom_type;
	guint8  charset;

	GString *name;
	GString *comment;
} network_mysqld_column_def;

network_mysqld_column_def *network_mysqld_column_def_new() {
	network_mysqld_column_def *column_def;

	column_def = g_new0(network_mysqld_column_def, 1);
	column_def->name    = g_string_new(NULL);
	column_def->comment = g_string_new(NULL);

	return column_def;
}

void network_mysqld_column_def_free(network_mysqld_column_def *column_def) {
	if (!column_def) return;

	g_string_free(column_def->name, TRUE);
	g_string_free(column_def->comment, TRUE);

	g_free(column_def);
}

typedef struct {
	guint8 row;
	GString *field_name;
} network_mysqld_screen_field;

network_mysqld_screen_field *network_mysqld_screen_field_new() {
	network_mysqld_screen_field *screen_field;

	screen_field = g_new0(network_mysqld_screen_field, 1);
	screen_field->field_name = g_string_new(NULL);

	return screen_field;
}

void network_mysqld_screen_field_free(network_mysqld_screen_field *screen_field) {
	if (!screen_field) return;

	g_string_free(screen_field->field_name, TRUE);

	g_free(screen_field);
}


typedef struct {
	guint16 len;
	guint8  fields_on_screen;
	guint8  start_row;
	guint8  col_1;

	GPtrArray *fields;
} network_mysqld_screen;

network_mysqld_screen *network_mysqld_screen_new() {
	network_mysqld_screen *screen;

	screen = g_new0(network_mysqld_screen, 1);
	screen->fields = g_ptr_array_new();

	return screen;
}

void network_mysqld_screen_free(network_mysqld_screen *screen) {
	guint i;
	if (!screen) return;

	for (i = 0; i < screen->fields->len; i++) {
		network_mysqld_screen_field_free(screen->fields->pdata[i]);
	}
	g_ptr_array_free(screen->fields, TRUE);

	g_free(screen);
}

typedef struct {
	guint8  frm_version;
	guint8  db_type;
	guint8  flags;
	guint16 key_info_offset;
	guint16 key_info_length;
	guint32 length;
	guint16 rec_length;
	guint32 max_rows;
	guint32 min_rows;

	guint8  new_field_pack_flag;
	guint32 key_length;
	guint16 table_options;
	guint32 avg_row_length;
	guint8  default_table_charset;
	guint8  row_type;
	guint32 mysqld_version;
	guint32 extra_size;
	guint16 extra_rec_buf_length;
	guint8  default_part_db_type;
	guint16 key_block_size;

	guint16 key_num;
	guint16 key_parts;
	guint16 key_extra_length;

	GPtrArray *keyinfo; 
	GPtrArray *columns; 
	GPtrArray *col_values; /* ENUM and SET map a int to a string */
	GPtrArray *screens;

	GString *connect_string;
	GString *se_name;
	GString *comment;

	guint16 col_count;
	guint16 col_names_len;
	guint16 col_values_count;   /* number of values for the ENUM/SET */
	guint16 col_values_parts;   /* ... */
	guint16 col_values_len;     /* byte-length of the ENUM-values */
	guint16 col_null_fields;
	guint16 col_comments_len;   /* byte-length of the comment-section */

	guint16 screens_len;        /* byte-length of the screens section */
	guint8  screens_count;      /* number of screens */
	guint16 __totlength;
	guint16 __no_empty;
	guint16 __rec_length;
	guint16 __time_stamp_pos;
	guint16 __cols_needed;
	guint16 __rows_needed;

	guint16 names_len;
	guint16 names_count;

	guint32 forminfo_offset;
	guint16 forminfo_len;
} network_mysqld_frm;

network_mysqld_frm *network_mysqld_frm_new() {
	network_mysqld_frm *frm;

	frm = g_new0(network_mysqld_frm, 1);
	frm->keyinfo = g_ptr_array_new();
	frm->columns = g_ptr_array_new();
	frm->col_values = g_ptr_array_new();
	frm->screens = g_ptr_array_new();

	frm->se_name        = g_string_new(NULL);
	frm->connect_string = g_string_new(NULL);
	frm->comment        = g_string_new(NULL);

	return frm;
}

void network_mysqld_frm_free(network_mysqld_frm *frm) {
	guint i;

	if (!frm) return;

	for (i = 0; i < frm->keyinfo->len; i++) {
		network_mysqld_keyinfo_free(frm->keyinfo->pdata[i]);
	}
	g_ptr_array_free(frm->keyinfo, TRUE);

	for (i = 0; i < frm->columns->len; i++) {
		network_mysqld_column_def_free(frm->columns->pdata[i]);
	}
	g_ptr_array_free(frm->columns, TRUE);

	for (i = 0; i < frm->screens->len; i++) {
		network_mysqld_screen_free(frm->screens->pdata[i]);
	}
	g_ptr_array_free(frm->screens, TRUE);

	for (i = 0; i < frm->col_values->len; i++) {
		guint j;
		GPtrArray *values = frm->col_values->pdata[i];

		for (j = 0; j < values->len; j++) {
			g_string_free(values->pdata[i], TRUE);
		}
		g_ptr_array_free(values, TRUE);
	}
	g_ptr_array_free(frm->col_values, TRUE);

	g_string_free(frm->se_name, TRUE);
	g_string_free(frm->connect_string, TRUE);
	g_string_free(frm->comment, TRUE);

	g_free(frm);
}


int network_mysqld_proto_get_frm(network_packet *packet, network_mysqld_frm *frm) {
	guint i;
	int err = 0;
	guint8 key_num_len;
	guint16 u16_key_length;
	guint32 u32_key_length; /* used if u16_key_info_length == 0xffff */
	guint8 __dummy;

	if (packet->data->str[0] != '\xfe' ||
	    packet->data->str[1] != '\x01') {

		g_critical("%s: frm-header should be: %02x%02x, got %02x%02x",
				G_STRLOC,
				'\xfe', '\x01',
				packet->data->str[0],
				packet->data->str[1]
				);

		g_return_val_if_reached(-1);
	}

	err = err || network_mysqld_proto_skip(packet, 2);
	err = err || network_mysqld_proto_get_int8(packet,  &frm->frm_version);
	err = err || network_mysqld_proto_get_int8(packet,  &frm->db_type);
	err = err || network_mysqld_proto_get_int16(packet, &frm->names_len); /* table.cc -> get_form_pos().length */
	err = err || network_mysqld_proto_get_int16(packet, &frm->key_info_offset); /* a pointer into this file */
	err = err || network_mysqld_proto_get_int16(packet, &frm->names_count); /* table.cc -> get_form_pos().names */
	err = err || network_mysqld_proto_get_int32(packet, &frm->length);
	err = err || network_mysqld_proto_get_int16(packet, &u16_key_length); /* if key_length > 0xffff stored in u32_key_length */
	err = err || network_mysqld_proto_get_int16(packet, &frm->rec_length);
	err = err || network_mysqld_proto_get_int32(packet, &frm->max_rows);
	err = err || network_mysqld_proto_get_int32(packet, &frm->min_rows);
	err = err || network_mysqld_proto_get_int8(packet,  &frm->flags); /* 0: system-table, 1: crypted */
	err = err || network_mysqld_proto_get_int8(packet,  &frm->new_field_pack_flag); /* new_field_pack_flag */
	err = err || network_mysqld_proto_get_int16(packet, &frm->key_info_length);
	err = err || network_mysqld_proto_get_int16(packet, &frm->table_options); /* db_create_options ( create_info->table_options) HA_OPTION_* */
	err = err || network_mysqld_proto_skip(packet, 1); /* always 00 */
	err = err || network_mysqld_proto_skip(packet, 1); /* major-mysql-version: 05 */
	err = err || network_mysqld_proto_get_int32(packet, &frm->avg_row_length);
	err = err || network_mysqld_proto_get_int8(packet,  &frm->default_table_charset);
	err = err || network_mysqld_proto_skip(packet, 1); /* 0-1: transactional, 2-3: page_checksum */
	err = err || network_mysqld_proto_get_int8(packet,  &frm->row_type);
	err = err || network_mysqld_proto_skip(packet, 6); /* always 00 */
	err = err || network_mysqld_proto_get_int32(packet, &u32_key_length);
	err = err || network_mysqld_proto_get_int32(packet, &frm->mysqld_version);
	err = err || network_mysqld_proto_get_int32(packet, &frm->extra_size);
	err = err || network_mysqld_proto_get_int16(packet, &frm->extra_rec_buf_length);
	err = err || network_mysqld_proto_get_int8(packet,  &frm->default_part_db_type);
	err = err || network_mysqld_proto_get_int16(packet, &frm->key_block_size); 

	err = err || network_mysqld_proto_skip(packet, frm->names_len); /* skip the "form-names" (table.cc -> get_form_pos()) */
	err = err || network_mysqld_proto_get_int32(packet, &frm->forminfo_offset);

	/* key info */
	err = err || (frm->key_info_offset > packet->data->len);
	err = err || (frm->key_info_offset + frm->key_info_length > packet->data->len);
	if (err) return -1;

	frm->key_length = (u16_key_length == 0xffff) ? u32_key_length : u16_key_length;

	packet->offset = frm->key_info_offset;

	err = err || network_mysqld_proto_peek_int8(packet,  &key_num_len);
	if (err) return -1;

	/* if we have less than 128 keys, we use one byte, if more two bytes */
	if (key_num_len < 128) {
		guint8 key_num0;
		guint8 key_parts0;

		err = err || network_mysqld_proto_get_int8(packet,  &key_num0);
		err = err || network_mysqld_proto_get_int8(packet,  &key_parts0);
		frm->key_num   = key_num0;
		frm->key_parts = key_parts0;

		network_mysqld_proto_skip(packet, 2);
	} else {
		guint8 key_num0;
		guint8 key_num1;

		err = err || network_mysqld_proto_get_int8(packet,  &key_num0);
		err = err || network_mysqld_proto_get_int8(packet,  &key_num1);
		err = err || network_mysqld_proto_get_int16(packet, &frm->key_parts);

		frm->key_num   = (key_num0 & 0x7f) | (key_num1 << 7);
	}
	err = err || network_mysqld_proto_get_int16(packet, &frm->key_extra_length);

	for (i = 0; i < frm->key_num; i++) {
		guint j;
		network_mysqld_keyinfo *key;

		key = network_mysqld_keyinfo_new();

		err = err || network_mysqld_proto_get_int16(packet, &key->flags);
		err = err || network_mysqld_proto_get_int16(packet, &key->key_length);
		err = err || network_mysqld_proto_get_int8(packet, &key->key_parts);
		err = err || network_mysqld_proto_get_int8(packet, &key->algorithm);
		err = err || network_mysqld_proto_get_int16(packet, &key->block_size);

		for (j = 0; j < key->key_parts; j++) {
			network_mysqld_keypart *part;

			part = network_mysqld_keypart_new();

			err = err || network_mysqld_proto_get_int16(packet, &part->fieldnr);
			if (!err) part->fieldnr &= 16383; /* FIELD_NR_MASK */
			err = err || network_mysqld_proto_get_int16(packet, &part->offset);
			if (!err) part->offset -= 1;
			err = err || network_mysqld_proto_get_int8(packet,  &part->flags);
			err = err || network_mysqld_proto_get_int16(packet, &part->key_type);
			err = err || network_mysqld_proto_get_int16(packet, &part->length);

			g_ptr_array_add(key->parts, part);
		}

		g_ptr_array_add(frm->keyinfo, key);
	}

	err = err || network_mysqld_proto_get_int8(packet, &__dummy);
	err = err || (__dummy != (guchar)'\xff'); /* NAMES_SEP_CHAR */

	/* get the keynames */
	for (i = 0; i < frm->key_num; i++) {
		guint pos;

		network_mysqld_keyinfo *key = frm->keyinfo->pdata[i];

		/* find the next NAMES_SEP_CHAR as string-term */
		err = err || network_mysqld_proto_find_int8(packet, '\xff', &pos);
		if (pos) err = err || network_mysqld_proto_get_gstring_len(packet, pos - 1, key->name);
		err = err || network_mysqld_proto_skip(packet, 1); /* skip the \xff */
	}


	if (frm->rec_length) {
		guint8 comment_length;

		/* forminfo */
		packet->offset = frm->forminfo_offset;
		
		err = err || network_mysqld_proto_get_int16(packet, &frm->forminfo_len);
	
		err = err || network_mysqld_proto_skip(packet, 44);
		err = err || network_mysqld_proto_get_int8(packet, &comment_length);
		err = err || network_mysqld_proto_get_gstring_len(packet, comment_length, frm->comment);

		packet->offset = frm->forminfo_offset + 256;
		err = err || network_mysqld_proto_get_int8(packet, &frm->screens_count);
		err = err || network_mysqld_proto_skip(packet, 1);

		err = err || network_mysqld_proto_get_int16(packet, &frm->col_count);   /* number of fields (field_nr) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->screens_len);  /* length of the screens section (info_length) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->__totlength);    /* (totlength) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->__no_empty);     /* (no_empty) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->__rec_length);   /* (rec_length) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->col_names_len); /* length of the names-section ( n_length) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->col_values_count); /* ( interval_count ) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->col_values_parts); /* ( interval_parts ) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->col_values_len); /* length of the enum-values section ( int_length) */
		err = err || network_mysqld_proto_get_int16(packet, &frm->__time_stamp_pos); /*  */
		err = err || network_mysqld_proto_get_int16(packet, &frm->__cols_needed); /*  */
		err = err || network_mysqld_proto_get_int16(packet, &frm->__rows_needed); /*  */
		err = err || network_mysqld_proto_get_int16(packet, &frm->col_null_fields); /* + 282 */
		err = err || network_mysqld_proto_get_int16(packet, &frm->col_comments_len);  /* length of the comments section */
		err = err || network_mysqld_proto_skip(packet, 2); /* + 286 -> 288 */

		g_assert_cmpint(frm->forminfo_offset + 288, ==, packet->offset);

		/* screens */
		for (i = 0; i < frm->screens_count; i++) {
			network_mysqld_screen *screen;
			guint8 s_len;
			guint j;

			screen = network_mysqld_screen_new();
			
			err = err || network_mysqld_proto_get_int16(packet, &screen->len);
			err = err || network_mysqld_proto_skip(packet, 1);
			err = err || network_mysqld_proto_get_int8(packet, &screen->fields_on_screen);
			err = err || network_mysqld_proto_get_int8(packet, &screen->start_row);
			err = err || network_mysqld_proto_skip(packet, 1);
			err = err || network_mysqld_proto_get_int8(packet, &screen->col_1);
			err = err || network_mysqld_proto_skip(packet, screen->col_1); /* well, those are just space */

			for (j = 0; j < screen->fields_on_screen; j++) {
				network_mysqld_screen_field *fld;

				fld = network_mysqld_screen_field_new();

				err = err || network_mysqld_proto_get_int8(packet, &fld->row);
				err = err || network_mysqld_proto_skip(packet, 1);
				err = err || network_mysqld_proto_get_int8(packet, &s_len);
				err = err || network_mysqld_proto_get_gstring_len(packet, s_len - 1, fld->field_name);
				err = err || network_mysqld_proto_skip(packet, 1); /* skip the trailing zero */

				g_ptr_array_add(screen->fields, fld);
			}
			
			g_ptr_array_add(frm->screens, screen);
		}


		/* array of ( fields * field_pack_length ) */
		for (i = 0; i < frm->col_count; i++) {
			network_mysqld_column_def *col;
			guint8 u8_p14;

			col = network_mysqld_column_def_new();

			err = err || network_mysqld_proto_skip(packet, 3);
			err = err || network_mysqld_proto_get_int16(packet, &col->field_len);
			err = err || network_mysqld_proto_get_int24(packet, &col->rec_pos);
			err = err || network_mysqld_proto_get_int16(packet, &col->pack_flags);
			err = err || network_mysqld_proto_get_int8(packet, &col->unireg_type);
			err = err || network_mysqld_proto_skip(packet, 1);
			err = err || network_mysqld_proto_get_int8(packet, &col->col_values_ndx);
			err = err || network_mysqld_proto_get_int8(packet, &col->field_type);
			err = err || network_mysqld_proto_get_int8(packet, &u8_p14); /* geom_type || charset */
			err = err || network_mysqld_proto_get_int16(packet, &col->comment_len);

			if (!err) {
				if (u8_p14 == MYSQL_TYPE_GEOMETRY) {
					col->geom_type = u8_p14;
				} else {
					col->charset = u8_p14;
				}
			}

			g_ptr_array_add(frm->columns, col);
		}

		err = err || network_mysqld_proto_get_int8(packet, &__dummy);
		err = err || (__dummy != (guchar)'\xff'); /* NAMES_SEP_CHAR */

		/* next are the column-names */
		for (i = 0; i < frm->columns->len; i++) {
			guint pos;

			network_mysqld_column_def *col = frm->columns->pdata[i];

			/* find the next NAMES_SEP_CHAR as string-term */
			err = err || network_mysqld_proto_find_int8(packet, '\xff', &pos);
			if (!err && pos) {
				err = err || network_mysqld_proto_get_gstring_len(packet, pos - 1, col->name);
			}
			err = err || network_mysqld_proto_skip(packet, 1); /* skip the \xff */
		}
		err = err || network_mysqld_proto_skip(packet, 1); /* the term-nul */

		/* enum-values
		 *
		 * list of enum|set values 
		 * */
		for (i = 0; i < frm->col_values_count; i++) {
			GPtrArray *v = NULL;

			err = err || network_mysqld_proto_get_int8(packet, &__dummy);
			err = err || (__dummy != (guchar)'\xff'); /* NAMES_SEP_CHAR */

			do {
				GString *s;
				guint pos;
				guint8 term_nul;

				err = err || network_mysqld_proto_peek_int8(packet, &term_nul);
				if (!err && (term_nul == '\x00')) {
					err = err || network_mysqld_proto_skip(packet, 1); /* the term-nul */
					break;
				}
		
				s = g_string_new("");	
				err = err || network_mysqld_proto_find_int8(packet, '\xff', &pos);
				if (!err && pos) {
					err = err || network_mysqld_proto_get_gstring_len(packet, pos - 1, s);
				}
				err = err || network_mysqld_proto_skip(packet, 1); /* skip the \xff */

				if (!v) v = g_ptr_array_new();
				g_ptr_array_add(v, s);
			} while (!err);

			if (v) g_ptr_array_add(frm->col_values, v);
		}

		/* next are the column-comments */
		g_debug_hexdump(G_STRLOC, packet->data->str + packet->offset, packet->data->len - packet->offset);
	}

	if (frm->extra_size) {
		guint16 connect_string_len;
		guint16 se_name_len;

		packet->offset = frm->key_info_offset + frm->key_length + frm->rec_length; /* this block is reclength */

		err = err || network_mysqld_proto_get_int16(packet, &connect_string_len);
		err = err || network_mysqld_proto_get_gstring_len(packet, connect_string_len, frm->connect_string);
		
		err = err || network_mysqld_proto_get_int16(packet, &se_name_len);
		err = err || network_mysqld_proto_get_gstring_len(packet, se_name_len,        frm->se_name);
	}

	return err ? -1 : 0;
}

void network_mysqld_frm_print(network_mysqld_frm *frm) {
	guint i;

#define V0_C(st, x, format, comment) g_print("  %-22s: %" format " %s\n", #x, st->x, comment)
#define V0_S(st, x, format) g_print("  %-22s: '%" format "'\n", #x, st->x->len ? st->x->str : "")
#define V0(st, x, format) V0_C(st, x, format, "")

#define V1_C_raw(var, name, format, comment) g_print("  [%d] %-18s: %" format " %s\n", i, name, var, comment)
#define V1_raw(var, name, format) V1_C_raw(var, name, format, "")
#define V1_C(st, x, format, comment) V1_C_raw(st->x, #x, format, comment)
#define V1_S(st, x, format) V1_C_raw(st->x->len ? st->x->str : "", #x, format, "")
#define V1(st, x, format) V1_C(st, x, format, "")

#define V2_C(st, x, format, comment) g_print("    [%d] %-16s: %" format " %s\n", j, #x, st->x, comment)
#define V2_S_raw(var, name, format) g_print("    [%d] %-16s: '%" format "'\n", j, name, var->len ? var->str : "")
#define V2_S(st, x, format) V2_S_raw(st->x, #x, format)
#define V2(st, x, format) V2_C(st, x, format, "")

	g_print("frm-Header\n");
	V0(frm, frm_version,    "d");
	V0(frm, db_type,        "d");
	V0(frm, flags,          "d");
	V0_C(frm, key_info_offset, "d", "-- absolute offset of the key-info-block");
	V0(frm, key_info_length, G_GUINT32_FORMAT);
	V0(frm, names_len,      "d");
	V0(frm, names_count,    "d");
	V0(frm, length,         "d");
	V0(frm, rec_length,     "d");
	V0(frm, max_rows,       G_GUINT32_FORMAT);
	V0(frm, min_rows,       G_GUINT32_FORMAT);
	V0(frm, new_field_pack_flag, "d");
	V0(frm, key_length,     "d");
	V0(frm, table_options,  "d");
	V0(frm, avg_row_length, G_GUINT32_FORMAT);
	V0(frm, default_table_charset, "d");
	V0(frm, row_type,       "d");
	V0(frm, mysqld_version, G_GUINT32_FORMAT);
	V0(frm, extra_size,     G_GUINT32_FORMAT);
	V0(frm, extra_rec_buf_length, "d");
	V0(frm, default_part_db_type, "d");
	V0(frm, key_block_size, "d");
	V0_C(frm, forminfo_offset, "d", "-- absolute offset of the form-info-block");
	V0_C(frm, forminfo_len, "d", "-- len of the form-info-block");
	
	g_print("Screens\n");
	V0_C(frm, screens_len, "d", "-- length of the screens block");
	V0(frm, screens_count, "d");
	V0(frm, __totlength, "d");
	V0(frm, __rec_length, "d");
	V0(frm, __no_empty, "d");
	V0(frm, __time_stamp_pos, "d");
	V0(frm, __rows_needed, "d");
	V0(frm, __cols_needed, "d");
	for (i = 0; i < frm->screens->len; i++) {
		network_mysqld_screen *screen = frm->screens->pdata[i];
		guint j;

		g_print("\n");
		V1(screen, start_row,     "d");
		V1(screen, fields_on_screen, "d");
		V1(screen, col_1,    "d");

		for (j = 0; j < screen->fields->len; j++) {
			network_mysqld_screen_field *fld = screen->fields->pdata[j];

			V2_S(fld, field_name, "s");
			V2(fld, row,        "d");
		}
	}

	g_print("Colums\n");
	V0_C(frm, col_count, "d", "-- number of fields on the table");
	V0_C(frm, col_names_len, "d", "-- length of the col_names block");
	V0(frm, col_values_count, "d");
	V0(frm, col_values_parts, "d");
	V0(frm, col_values_len, "d");
	V0(frm, col_null_fields, "d");
	V0(frm, col_comments_len, "d");

	for (i = 0; i < frm->columns->len; i++) {
		network_mysqld_column_def *col = frm->columns->pdata[i];
		g_print("\n");
		V1_S(col, name,        "s");
		V1_C(col, field_len,   "d", "-- display width for INT");
		V1(col, rec_pos,       "d");
		V1_raw(network_mysqld_proto_field_get_typestring(col->field_type), "field_type", "s");
		V1(col, charset,       "d");
		V1(col, unireg_type,   "d");
		V1(col, pack_flags,    "x");
		V1(col, comment_len,   "d");
		V1(col, col_values_ndx, "d");
		if (col->col_values_ndx) {
			guint j;
			GPtrArray *v;

			g_assert_cmpint(col->col_values_ndx, >, 0);
			g_assert_cmpint(col->col_values_ndx - 1, <, frm->col_values->len);

			v = frm->col_values->pdata[col->col_values_ndx - 1];

			for (j = 0; j < v->len; j++) {
				GString *s = v->pdata[j];

				V2_S_raw(s, "enum|set", "s");
			}
		}
	}

	g_print("Keys\n");
	V0(frm, key_num,        "d");
	V0_C(frm, key_parts,      "d", "-- has to be >= than the keyparts of each key");
	V0(frm, key_extra_length, "d");

	for (i = 0; i < frm->keyinfo->len; i++) {
		guint j;
		network_mysqld_keyinfo *key = frm->keyinfo->pdata[i];

		V1_S(key, name,      "s");
		V1(key, flags,       "x");
		V1(key, key_length,  "d");
		V1(key, key_parts,   "d");
		V1(key, algorithm,   "d");
		V1(key, block_size,  "d");

		for (j = 0; j < key->parts->len; j++) {
			network_mysqld_keypart *part = key->parts->pdata[j];
			g_print("\n");
			V2(part, fieldnr,     "d");
			V2(part, offset,      "d");
			V2(part, key_type,    "d");
			V2(part, flags,       "x");
			V2(part, length,      "d");
		}
	}
	
	g_print("Extra\n");
	V0_S(frm, connect_string, "s");
	V0_S(frm, se_name, "s");
	V0_S(frm, comment, "s");
}

void network_mysqld_myd_print(network_mysqld_frm G_GNUC_UNUSED *frm, const char *filename) {
	GMappedFile *f;
	GError *gerr = NULL;
	network_packet *packet;

	if (!filename) return;

	f = g_mapped_file_new(filename, FALSE, &gerr);
	if (!f) {
		g_critical("%s: %s",
				G_STRLOC,
				gerr->message);
		g_error_free(gerr);
		return;
	}

	packet = network_packet_new();
	packet->data = g_string_new(NULL);

	packet->data->str = g_mapped_file_get_contents(f);
	packet->data->len = g_mapped_file_get_length(f);

	g_debug_hexdump(G_STRLOC, S(packet->data));



	g_mapped_file_free(f);

	g_string_free(packet->data, FALSE);
	network_packet_free(packet);
}
/**
 * read a frm file
 */
int frm_dump_file(
		const char *filename,
		const char *myd_filename) {
	network_packet *packet;
	GMappedFile *f;
	GError *gerr = NULL;
	network_mysqld_frm *frm;
	int err = 0;

	f = g_mapped_file_new(filename, FALSE, &gerr);
	if (!f) {
		g_critical("%s: %s",
				G_STRLOC,
				gerr->message);
		g_error_free(gerr);
		return -1;
	}

	packet = network_packet_new();
	packet->data = g_string_new(NULL);

	packet->data->str = g_mapped_file_get_contents(f);
	packet->data->len = g_mapped_file_get_length(f);

	frm = network_mysqld_frm_new();
	err = err || network_mysqld_proto_get_frm(packet, frm);
	if (!err) {
		network_mysqld_frm_print(frm);
		network_mysqld_myd_print(frm, myd_filename);
	}

	g_mapped_file_free(f);

	g_string_free(packet->data, FALSE);
	network_packet_free(packet);

	return err ? -1 : 0;
}

#define GETTEXT_PACKAGE "mysql-myisam-dump"

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
	gchar *frm_filename = NULL;
	gchar *myd_filename = NULL;

	GKeyFile *keyfile = NULL;
	chassis_log *log;

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
		
		{ "frm-file",                 0, 0, G_OPTION_ARG_FILENAME, NULL, "frm filename", "<file>" },
		{ "myd-file",                 0, 0, G_OPTION_ARG_FILENAME, NULL, "myd filename", "<file>" },
		
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
	main_entries[i++].arg_data  = &(frm_filename);
	main_entries[i++].arg_data  = &(myd_filename);

	option_ctx = g_option_context_new("- MySQL MyISAM Dump");
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
		if (chassis_keyfile_to_options(keyfile, "mysql-myisam-dump", main_entries)) {
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

	if (!frm_filename) {
		exit_code = EXIT_FAILURE;
		goto exit_nicely;
	}

	if (frm_dump_file(frm_filename, myd_filename)) {
		exit_code = EXIT_FAILURE;
		goto exit_nicely;
	}

exit_nicely:
	if (option_ctx) g_option_context_free(option_ctx);
	if (keyfile) g_key_file_free(keyfile);
	if (default_file) g_free(default_file);
	if (frm_filename) g_free(frm_filename);
	if (gerr) g_error_free(gerr);

	if (log_level) g_free(log_level);
	if (chas) chassis_free(chas);
	
	chassis_log_free(log);

	return exit_code;
}


