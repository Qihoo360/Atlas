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
 * codec's for the binary MySQL client protocol
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "network_mysqld_proto_binary.h"

#include "glib-ext.h"
#include "string-len.h"

/* ints */
static int network_mysqld_proto_binary_get_int_type(network_packet *packet, network_mysqld_type_t *type) {
	int err = 0;
	guint8 i8;
	guint16 i16;
	guint32 i32;
	guint64 i64;

	switch (type->type) {
	case MYSQL_TYPE_TINY:
		err = err || network_mysqld_proto_get_int8(packet, &i8);
		err = err || network_mysqld_type_set_int(type, (guint64)i8, type->is_unsigned);
		break;
	case MYSQL_TYPE_SHORT:
		err = err || network_mysqld_proto_get_int16(packet, &i16);
		err = err || network_mysqld_type_set_int(type, (guint64)i16, type->is_unsigned);
		break;
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_INT24:
		err = err || network_mysqld_proto_get_int32(packet, &i32);
		err = err || network_mysqld_type_set_int(type, (guint64)i32, type->is_unsigned);
		break;
	case MYSQL_TYPE_LONGLONG:
		err = err || network_mysqld_proto_get_int64(packet, &i64);
		err = err || network_mysqld_type_set_int(type, i64, type->is_unsigned);
		break;
	default:
		err = -1;
		break;
	}

	return err ? -1 : 0;
}

static int network_mysqld_proto_binary_append_int_type(GString *packet, network_mysqld_type_t *type) {
	guint64 i64;
	guint8  i8;
	guint16 i16;
	guint32 i32;
	int err = 0;

	err = err || network_mysqld_type_get_int(type, &i64, NULL);
	if (0 != err) return -1;

	switch (type->type) {
	case MYSQL_TYPE_TINY:

		i8 = i64;

		err = err || network_mysqld_proto_append_int8(packet, i8);
		break;
	case MYSQL_TYPE_SHORT:
		i16 = i64;

		err = err || network_mysqld_proto_append_int16(packet, i16);
		break;
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_INT24:
		i32 = i64;

		err = err || network_mysqld_proto_append_int32(packet, i32);
		break;
	case MYSQL_TYPE_LONGLONG:
		err = err || network_mysqld_proto_append_int64(packet, i64);
		break;
	default:
		err = -1;
		break;
	}

	return err ? -1 : 0;
}

/* double */
static int network_mysqld_proto_binary_get_double_type(network_packet *packet, network_mysqld_type_t *type) {
	int err = 0;
	union {
		double d;
		guint8 d_char_shadow[sizeof(double)];
	} double_copy;
	unsigned long i;

#ifdef WORDS_BIGENDIAN
	/* big endian: ppc, ... */
	for (i = 0; 0 == err && i < sizeof(double); i++) {
		err = err || network_mysqld_proto_get_int8(packet, &(double_copy.d_char_shadow[sizeof(double) - 1 - i]));
	}
#else
	/* little endian: x86: network == system byte-order */
	for (i = 0; 0 == err && i < sizeof(double); i++) {
		err = err || network_mysqld_proto_get_int8(packet, &(double_copy.d_char_shadow[i]));
	}
#endif

	err = err || network_mysqld_type_set_double(type, double_copy.d);

	return err ? -1 : 0;
}

static int network_mysqld_proto_binary_append_double_type(GString *packet, network_mysqld_type_t *type) {
	union {
		double d;
		guint8 d_char_shadow[sizeof(double)];
	} double_copy;
	int err = 0;
	unsigned long i;

	err = err || network_mysqld_type_get_double(type, &double_copy.d);
	if (0 != err) return -1;

#ifdef WORDS_BIGENDIAN
	/* big endian: ppc, ... */
	for (i = 0; 0 == err && i < sizeof(double); i++) {
		err = err || network_mysqld_proto_append_int8(packet, double_copy.d_char_shadow[sizeof(double) - 1 - i]);
	}
#else
	/* little endian: x86: network == system byte-order */
	for (i = 0; 0 == err && i < sizeof(double); i++) {
		err = err || network_mysqld_proto_append_int8(packet, double_copy.d_char_shadow[i]);
	}
#endif

	return err ? -1 : 0;
}

/* float */
static int network_mysqld_proto_binary_get_float_type(network_packet *packet, network_mysqld_type_t *type) {
	int err = 0;
	union {
		float f;
		guint8 f_char_shadow[sizeof(float)];
	} float_copy;
	unsigned long i;
	double d;

#ifdef WORDS_BIGENDIAN
	/* big endian: ppc, ... */
	for (i = 0; 0 == err && i < sizeof(float); i++) {
		err = err || network_mysqld_proto_get_int8(packet, &(float_copy.f_char_shadow[sizeof(float) - 1 - i]));
	}
#else
	/* little endian: x86: network == system byte-order */
	for (i = 0; 0 == err && i < sizeof(float); i++) {
		err = err || network_mysqld_proto_get_int8(packet, &(float_copy.f_char_shadow[i]));
	}
#endif

	d = float_copy.f;

	err = err || network_mysqld_type_set_double(type, d);

	return err ? -1 : 0;
}

static int network_mysqld_proto_binary_append_float_type(GString *packet, network_mysqld_type_t *type) {
	union {
		float f;
		guint8 f_char_shadow[sizeof(float)];
	} float_copy;
	double d;
	int err = 0;
	unsigned long i;

	err = err || network_mysqld_type_get_double(type, &d);
	if (0 != err) return -1;

	float_copy.f = d;

#ifdef WORDS_BIGENDIAN
	/* big endian: ppc, ... */
	for (i = 0; 0 == err && i < sizeof(float); i++) {
		err = err || network_mysqld_proto_append_int8(packet, float_copy.f_char_shadow[sizeof(float) - 1 - i]);
	}
#else
	/* little endian: x86: network == system byte-order */
	for (i = 0; 0 == err && i < sizeof(float); i++) {
		err = err || network_mysqld_proto_append_int8(packet, float_copy.f_char_shadow[i]);
	}
#endif

	return err ? -1 : 0;
}

/* all kinds of strings */
static int network_mysqld_proto_binary_get_string_type(network_packet *packet, network_mysqld_type_t *type) {
	GString *str;
	int err = 0;

	str = g_string_new(NULL);

	err = err || network_mysqld_proto_get_lenenc_gstring(packet, str);

	network_mysqld_type_set_string(type, S(str));

	g_string_free(str, TRUE);

	return err ? -1 : 0;
}

static int network_mysqld_proto_binary_append_string_type(GString *packet, network_mysqld_type_t *type) {
	const char *s;
	gsize s_len;
	int err = 0;

	err = err || network_mysqld_type_get_string_const(type, &s, &s_len);
	err = err || network_mysqld_proto_append_lenenc_string_len(packet, s, s_len);

	return err ? -1 : 0;
}

/* all kinds of time */

/**
 * extract the date from a binary resultset row
 */
static int network_mysqld_proto_binary_get_date_type(network_packet *packet, network_mysqld_type_t *type) {
	int err = 0;
	guint8 len;
	network_mysqld_type_date_t date;

	err = err || network_mysqld_proto_get_int8(packet, &len);

	/* check the valid lengths
	 */
	switch (len) {
	case 11: /* date + time + ms */
	case 7:  /* date + time ( ms is .0000 ) */
	case 4:  /* date ( time is 00:00:00 )*/
	case 0:  /* date == 0000-00-00 */
		break;
	default:
		return -1;
	}

	memset(&date, 0, sizeof(date));
	if (len > 0) {
		err = err || network_mysqld_proto_get_int16(packet, &date.year);
		err = err || network_mysqld_proto_get_int8(packet, &date.month);
		err = err || network_mysqld_proto_get_int8(packet, &date.day);
		
		if (len > 4) {
			err = err || network_mysqld_proto_get_int8(packet, &date.hour);
			err = err || network_mysqld_proto_get_int8(packet, &date.min);
			err = err || network_mysqld_proto_get_int8(packet, &date.sec);

			if (len > 7) {
				err = err || network_mysqld_proto_get_int32(packet, &date.nsec);
			}
		}
	}

	if (0 == err) {
		err = err || network_mysqld_type_set_date(type, &date);
	}

	return err ? -1 : 0;
}

static int network_mysqld_proto_binary_append_date_type(GString G_GNUC_UNUSED *packet, network_mysqld_type_t G_GNUC_UNUSED *type) {
	/* not implemented yet */
	return -1;
}

/**
 * extract the time from a binary resultset row
 */
static int network_mysqld_proto_binary_get_time_type(network_packet *packet, network_mysqld_type_t *type) {
	int err = 0;
	guint8 len;
	network_mysqld_type_time_t t;

	err = err || network_mysqld_proto_get_int8(packet, &len);

	/* check the valid lengths
	 */
	switch (len) {
	case 12: /* day + time + ms */
	case 8:  /* day + time ( ms is .0000 ) */
	case 0:  /* time == 00:00:00 */
		break;
	default:
		return -1;
	}

	memset(&t, 0, sizeof(t));
	if (len > 0) {
		err = err || network_mysqld_proto_get_int8(packet, &t.sign);
		err = err || network_mysqld_proto_get_int32(packet, &t.days);
		
		err = err || network_mysqld_proto_get_int8(packet, &t.hour);
		err = err || network_mysqld_proto_get_int8(packet, &t.min);
		err = err || network_mysqld_proto_get_int8(packet, &t.sec);

		if (len > 8) {
			err = err || network_mysqld_proto_get_int32(packet, &t.nsec);
		}
	}

	if (0 == err) {
		err = err || network_mysqld_type_set_time(type, &t);
	}

	return err ? -1 : 0;
}

static int network_mysqld_proto_binary_append_time_type(GString G_GNUC_UNUSED *packet, network_mysqld_type_t G_GNUC_UNUSED *type) {
	/* not implemented yet */
	return -1;
}

/**
 * valid types for prepared statements parameters we receive from the client
 */
gboolean network_mysql_proto_binary_type_is_valid_input(enum enum_field_types field_type) {
	switch (field_type) {
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_LONGLONG:

	case MYSQL_TYPE_FLOAT:
	case MYSQL_TYPE_DOUBLE:

	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_STRING:

	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_TIMESTAMP:

	case MYSQL_TYPE_NULL:
		return TRUE;
	default:
		return FALSE;
	}
}

/**
 * types we allow the send back to the client
 */
gboolean network_mysql_proto_binary_is_valid_output(enum enum_field_types field_type) {
	switch (field_type) {
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_LONGLONG:

	case MYSQL_TYPE_FLOAT:
	case MYSQL_TYPE_DOUBLE:
	case MYSQL_TYPE_NEWDECIMAL:

	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_VAR_STRING:

	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_TIMESTAMP:

	case MYSQL_TYPE_BIT:
		return TRUE;
	default:
		return FALSE;
	}
}

int network_mysqld_proto_binary_get_type(network_packet *packet, network_mysqld_type_t *type) {
	switch (type->type) {
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONGLONG:
		return network_mysqld_proto_binary_get_int_type(packet, type);
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIMESTAMP:
		return network_mysqld_proto_binary_get_date_type(packet, type);
	case MYSQL_TYPE_TIME:
		return network_mysqld_proto_binary_get_time_type(packet, type);
	case MYSQL_TYPE_FLOAT:
		return network_mysqld_proto_binary_get_float_type(packet, type);
	case MYSQL_TYPE_DOUBLE:
		return network_mysqld_proto_binary_get_double_type(packet, type);
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_VAR_STRING:
		/* they are all length-encoded strings */
		return network_mysqld_proto_binary_get_string_type(packet, type);
	default:
		break;
	}

	return -1;
}

int network_mysqld_proto_binary_append_type(GString *packet, network_mysqld_type_t *type) {
	switch (type->type) {
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONGLONG:
		return network_mysqld_proto_binary_append_int_type(packet, type);
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIMESTAMP:
		return network_mysqld_proto_binary_append_date_type(packet, type);
	case MYSQL_TYPE_TIME:
		return network_mysqld_proto_binary_append_time_type(packet, type);
	case MYSQL_TYPE_FLOAT:
		return network_mysqld_proto_binary_append_float_type(packet, type);
	case MYSQL_TYPE_DOUBLE:
		return network_mysqld_proto_binary_append_double_type(packet, type);
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_VAR_STRING:
		/* they are all length-encoded strings */
		return network_mysqld_proto_binary_append_string_type(packet, type);
	default:
		break;
	}

	return -1;
}


