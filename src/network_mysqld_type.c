#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <glib.h>

#include "network_mysqld_type.h"
#include "string-len.h"

#include "glib-ext.h"

/* expose the types itself and their internal representation */

typedef double network_mysqld_type_double_t;

typedef float network_mysqld_type_float_t;

typedef GString network_mysqld_type_string_t;

typedef struct {
	guint64 i;
	gboolean is_unsigned;
} network_mysqld_type_int_t;

/**
 * create a type that can hold a MYSQL_TYPE_LONGLONG
 */
static network_mysqld_type_int_t *network_mysqld_type_int_new(void) {
	network_mysqld_type_int_t *ll;
	
	ll = g_slice_new0(network_mysqld_type_int_t);

	return ll;
}

/**
 * free a network_mysqld_type_int_t
 */
static void network_mysqld_type_int_free(network_mysqld_type_int_t *ll) {
	if (NULL == ll) return;

	g_slice_free(network_mysqld_type_int_t, ll);
}

static int network_mysqld_type_data_int_get_int(network_mysqld_type_t *type, guint64 *i, gboolean *is_unsigned) {
	network_mysqld_type_int_t *value;

	if (NULL == type->data) return -1;

	value = type->data;

	*i = value->i;
	*is_unsigned = value->is_unsigned;

	return 0;
}

static int network_mysqld_type_data_int_set_int(network_mysqld_type_t *type, guint64 i, gboolean is_unsigned) {
	network_mysqld_type_int_t *value;

	if (NULL == type->data) {
		type->data = network_mysqld_type_int_new();
	}	
	value = type->data;

	value->i = i;
	value->is_unsigned = is_unsigned;

	return 0;
}


/**
 * typesafe wrapper for network_mysqld_type_new()
 */
static void network_mysqld_type_data_int_free(network_mysqld_type_t *type) {
	if (NULL == type) return;
	if (NULL == type->data) return;

	network_mysqld_type_int_free(type->data);
}

static void network_mysqld_type_data_int_init(network_mysqld_type_t *type, enum enum_field_types field_type) {
	type->type	= field_type;
	type->free_data = network_mysqld_type_data_int_free;
	type->get_int    = network_mysqld_type_data_int_get_int;
	type->set_int    = network_mysqld_type_data_int_set_int;
}

/* MYSQL_TYPE_DOUBLE */

/**
 * create a type that can hold a MYSQL_TYPE_DOUBLE
 */
static network_mysqld_type_double_t *network_mysqld_type_double_new(void) {
	network_mysqld_type_double_t *t;
	
	t = g_slice_new0(network_mysqld_type_double_t);

	return t;
}

/**
 * free a network_mysqld_type_double_t
 */
static void network_mysqld_type_double_free(network_mysqld_type_double_t *t) {
	if (NULL == t) return;

	g_slice_free(network_mysqld_type_double_t, t);
}

static void network_mysqld_type_data_double_free(network_mysqld_type_t *type) {
	if (NULL == type) return;
	if (NULL == type->data) return;

	network_mysqld_type_double_free(type->data);
}

static int network_mysqld_type_data_double_get_double(network_mysqld_type_t *type, double *d) {
	network_mysqld_type_double_t *value = type->data;

	if (NULL == value) return -1;

	*d = *value;

	return 0;
}

static int network_mysqld_type_data_double_set_double(network_mysqld_type_t *type, double d) {
	network_mysqld_type_double_t *value;

	if (NULL == type->data) {
		type->data = network_mysqld_type_double_new();
	}

	value = type->data;
	*value = d;

	return 0;
}

static void network_mysqld_type_data_double_init(network_mysqld_type_t *type, enum enum_field_types field_type) {
	type->type	= field_type;
	type->free_data = network_mysqld_type_data_double_free;
	type->get_double = network_mysqld_type_data_double_get_double;
	type->set_double = network_mysqld_type_data_double_set_double;
}

/* MYSQL_TYPE_FLOAT */

/**
 * create a type that can hold a MYSQL_TYPE_FLOAT
 */

static network_mysqld_type_float_t *network_mysqld_type_float_new(void) {
	network_mysqld_type_float_t *t;
	
	t = g_slice_new0(network_mysqld_type_float_t);

	return t;
}

/**
 * free a network_mysqld_type_float_t
 */
static void network_mysqld_type_float_free(network_mysqld_type_float_t *t) {
	if (NULL == t) return;

	g_slice_free(network_mysqld_type_float_t, t);
}

static void network_mysqld_type_data_float_free(network_mysqld_type_t *type) {
	if (NULL == type) return;
	if (NULL == type->data) return;

	network_mysqld_type_float_free(type->data);
}

static int network_mysqld_type_data_float_get_double(network_mysqld_type_t *type, double *dst) {
	network_mysqld_type_float_t *src = type->data;

	if (NULL == type->data) return -1;

	*dst = (double)*src;

	return 0;
}

static int network_mysqld_type_data_float_set_double(network_mysqld_type_t *type, double src) {
	network_mysqld_type_float_t *dst = type->data;

	if (NULL == type->data) {
		type->data = network_mysqld_type_float_new();
	}

	dst = type->data;
	*dst = (float)src;

	return 0;
}

static void network_mysqld_type_data_float_init(network_mysqld_type_t *type, enum enum_field_types field_type) {
	type->type	= field_type;
	type->free_data = network_mysqld_type_data_float_free;
	type->get_double = network_mysqld_type_data_float_get_double;
	type->set_double = network_mysqld_type_data_float_set_double;
}

/* MYSQL_TYPE_STRING */
static network_mysqld_type_string_t *network_mysqld_type_string_new(void) {
	network_mysqld_type_string_t *str;

	str = g_string_new(NULL);

	return str;
}

static void network_mysqld_type_string_free(network_mysqld_type_string_t *str) {
	if (NULL == str) return;

	g_string_free(str, TRUE);
}

static void network_mysqld_type_data_string_free(network_mysqld_type_t *type) {
	if (NULL == type) return;

	network_mysqld_type_string_free(type->data);
}

static int network_mysqld_type_data_string_get_string_const(network_mysqld_type_t *type, const char **dst, gsize *dst_len) {
	GString *src = type->data;

	if (NULL == type->data) return -1;

	*dst = src->str;
	*dst_len = src->len;
	
	return 0;
}

static int network_mysqld_type_data_string_set_string(network_mysqld_type_t *type, const char *src, gsize src_len) {
	GString *dst;

	if (NULL == type->data) {
		type->data = g_string_sized_new(src_len);
	}

	dst = type->data;

	g_string_assign_len(dst, src, src_len);
	
	return 0;
}

static void network_mysqld_type_data_string_init(network_mysqld_type_t *type, enum enum_field_types field_type) {
	type->type	= field_type;
	type->free_data = network_mysqld_type_data_string_free;
	type->get_string_const = network_mysqld_type_data_string_get_string_const;
	type->set_string = network_mysqld_type_data_string_set_string;
}

/* MYSQL_TYPE_DATE */
static network_mysqld_type_date_t *network_mysqld_type_date_new(void) {
	network_mysqld_type_date_t *date;

	date = g_slice_new0(network_mysqld_type_date_t);

	return date;
}

static void network_mysqld_type_date_free(network_mysqld_type_date_t *date) {
	if (NULL == date) return;

	g_slice_free(network_mysqld_type_date_t, date);
}

static void network_mysqld_type_data_date_free(network_mysqld_type_t *type) {
	if (NULL == type) return;

	network_mysqld_type_date_free(type->data);
}

static int network_mysqld_type_data_date_get_date(network_mysqld_type_t *type, network_mysqld_type_date_t *dst) {
	network_mysqld_type_date_t *src = type->data;

	if (NULL == type->data) return -1;

	memcpy(dst, src, sizeof(*src));

	return 0;
}

static gboolean network_mysqld_type_date_time_is_valid(network_mysqld_type_date_t *date) {
	return (date->nsec < 1000000000 &&
	      date->sec  < 100 &&
	      date->min  <= 60 &&
	      date->hour <= 24);
}

static gboolean network_mysqld_type_date_date_is_valid(network_mysqld_type_date_t *date) {
	return (date->day <= 31 &&
	      date->month <= 12 &&
	      date->year  <= 9999);
}

gboolean network_mysqld_type_date_is_valid(network_mysqld_type_date_t *date) {
	return network_mysqld_type_date_time_is_valid(date) &&
		network_mysqld_type_date_date_is_valid(date);
}

static int network_mysqld_type_data_date_get_string(network_mysqld_type_t *type, char **dst, gsize *dst_len) {
	network_mysqld_type_date_t *src = type->data;

	if (NULL == type->data) return -1;

	switch (type->type) {
	case MYSQL_TYPE_DATE:
		if (!network_mysqld_type_date_date_is_valid(src)) {
			return -1;
		}
		break;
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_TIMESTAMP:
		if (!network_mysqld_type_date_is_valid(src)) {
			return -1;
		}
		break;
	default:
		/* we shouldn't be here */
		return -1;
	}

	if (NULL != *dst) {
		switch (type->type) {
		case MYSQL_TYPE_DATE:
			/* dst_len already contains a size and we don't have to alloc */
			if (*dst_len < NETWORK_MYSQLD_TYPE_DATE_MIN_BUF_LEN) {
				return -1; /* ... but it is too small .. we could return the right size here */
			}
			*dst_len = g_snprintf(*dst, *dst_len, "%04u-%02u-%02u",
					src->year,
					src->month,
					src->day);
			break;
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIMESTAMP:
			/* dst_len already contains a size and we don't have to alloc */
			if (*dst_len < NETWORK_MYSQLD_TYPE_DATETIME_MIN_BUF_LEN) {
				return -1; /* ... but it is too small .. we could return the right size here */
			}
			*dst_len = g_snprintf(*dst, *dst_len, "%04u-%02u-%02u %02u:%02u:%02u.%09u",
					src->year,
					src->month,
					src->day,
					src->hour,
					src->min,
					src->sec,
					src->nsec);
			break;
		default:
			g_assert_not_reached();
			break;
		}
	} else {
		switch (type->type) {
		case MYSQL_TYPE_DATE:
			*dst = g_strdup_printf("%04u-%02u-%02u",
					src->year,
					src->month,
					src->day);
			*dst_len = strlen(*dst);
			break;
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIMESTAMP:
			*dst = g_strdup_printf("%04u-%02u-%02u %02u:%02u:%02u.%09u",
					src->year,
					src->month,
					src->day,
					src->hour,
					src->min,
					src->sec,
					src->nsec);
			*dst_len = strlen(*dst);
			break;
		default:
			g_assert_not_reached();
			break;
		}
	}

	return 0;
}


static int network_mysqld_type_data_date_set_date(network_mysqld_type_t *type, network_mysqld_type_date_t *src) {
	network_mysqld_type_date_t *dst;

	if (NULL == type->data) {
		type->data = network_mysqld_type_date_new();
	}

	dst = type->data;

	memcpy(dst, src, sizeof(*src));

	return 0;
}

static void network_mysqld_type_data_date_init(network_mysqld_type_t *type, enum enum_field_types field_type) {
	type->type	= field_type;
	type->free_data = network_mysqld_type_data_date_free;
	type->get_date   = network_mysqld_type_data_date_get_date;
	type->get_string = network_mysqld_type_data_date_get_string;
	type->set_date   = network_mysqld_type_data_date_set_date;
}


/* MYSQL_TYPE_TIME */
static network_mysqld_type_time_t *network_mysqld_type_time_new(void) {
	network_mysqld_type_time_t *t;

	t = g_slice_new0(network_mysqld_type_time_t);

	return t;
}

static void network_mysqld_type_time_free(network_mysqld_type_time_t *t) {
	if (NULL == t) return;

	g_slice_free(network_mysqld_type_time_t, t);
}

static void network_mysqld_type_data_time_free(network_mysqld_type_t *type) {
	if (NULL == type) return;

	network_mysqld_type_time_free(type->data);
}

static int network_mysqld_type_data_time_get_time(network_mysqld_type_t *type, network_mysqld_type_time_t *dst) {
	network_mysqld_type_time_t *src = type->data;

	if (NULL == type->data) return -1;

	memcpy(dst, src, sizeof(*src));

	return 0;
}

static int network_mysqld_type_data_time_get_string(network_mysqld_type_t *type, char **dst, gsize *dst_len) {
	network_mysqld_type_time_t *src = type->data;

	if (NULL == type->data) return -1;

	if (NULL != *dst) {
		/* dst_len already contains a size and we don't have to alloc */
		if (*dst_len < NETWORK_MYSQLD_TYPE_TIME_MIN_BUF_LEN) {
			return -1; /* ... but it is too small .. we could return the right size here */
		}
		*dst_len = g_snprintf(*dst, *dst_len, "%s%d %02u:%02u:%02u.%09u",
				src->sign ? "-" : "",
				src->days,
				src->hour,
				src->min,
				src->sec,
				src->nsec);
	} else {
		*dst = g_strdup_printf("%s%d %02u:%02u:%02u.%09u",
				src->sign ? "-" : "",
				src->days,
				src->hour,
				src->min,
				src->sec,
				src->nsec);
		*dst_len = strlen(*dst);
	}

	return 0;
}

static int network_mysqld_type_data_time_set_time(network_mysqld_type_t *type, network_mysqld_type_time_t *src) {
	network_mysqld_type_date_t *dst;

	if (NULL == type->data) {
		type->data = network_mysqld_type_time_new();
	}
	dst = type->data;

	memcpy(dst, src, sizeof(*src));

	return 0;
}


static void network_mysqld_type_data_time_init(network_mysqld_type_t *type, enum enum_field_types field_type) {
	type->type	= field_type;
	type->free_data = network_mysqld_type_data_time_free;
	type->get_time = network_mysqld_type_data_time_get_time;
	type->get_string = network_mysqld_type_data_time_get_string;
	type->set_time = network_mysqld_type_data_time_set_time;
}


/**
 * create a type 
 */
network_mysqld_type_t *network_mysqld_type_new(enum enum_field_types field_type) {
	network_mysqld_type_t *type = NULL;

	switch (field_type) {
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONGLONG:
		type = g_slice_new0(network_mysqld_type_t);

		network_mysqld_type_data_int_init(type, field_type);
		break;
	case MYSQL_TYPE_FLOAT: /* 4 bytes */
		type = g_slice_new0(network_mysqld_type_t);

		network_mysqld_type_data_float_init(type, field_type);
		break;
	case MYSQL_TYPE_DOUBLE: /* 8 bytes */
		type = g_slice_new0(network_mysqld_type_t);

		network_mysqld_type_data_double_init(type, field_type);
		break;
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_TIMESTAMP:
		type = g_slice_new0(network_mysqld_type_t);

		network_mysqld_type_data_date_init(type, field_type);
		break;
	case MYSQL_TYPE_TIME:
		type = g_slice_new0(network_mysqld_type_t);

		network_mysqld_type_data_time_init(type, field_type);
		break;
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_VARCHAR:
		/* they are all length-encoded strings */
		type = g_slice_new0(network_mysqld_type_t);

		network_mysqld_type_data_string_init(type, field_type);
		break;
	case MYSQL_TYPE_NULL:
		type = g_slice_new0(network_mysqld_type_t);

		type->type = field_type;
		break;
	default:
		break;
	}

	return type;
}
/**
 * free a type
 */
void network_mysqld_type_free(network_mysqld_type_t *type) {
	if (NULL == type) return;

	if (NULL != type->free_data) {
		type->free_data(type);
	}
	g_slice_free(network_mysqld_type_t, type);
}

int network_mysqld_type_get_gstring(network_mysqld_type_t *type, GString *s) {
	if (NULL == type->get_gstring) return -1;

	return type->get_gstring(type, s);
}

int network_mysqld_type_get_string_const(network_mysqld_type_t *type, const char **s, gsize *s_len) {
	if (NULL == type->get_string_const) return -1;

	return type->get_string_const(type, s, s_len);
}

int network_mysqld_type_get_string(network_mysqld_type_t *type, char **s, gsize *s_len) {
	if (NULL == type->get_string) return -1;

	return type->get_string(type, s, s_len);
}


int network_mysqld_type_set_string(network_mysqld_type_t *type, const char *s, gsize s_len) {
	if (NULL == type->set_string) return -1;

	return type->set_string(type, s, s_len);
}


int network_mysqld_type_get_int(network_mysqld_type_t *type, guint64 *i, gboolean *is_unsigned) {
	if (NULL == type->get_int) return -1;

	return type->get_int(type, i, is_unsigned);
}


int network_mysqld_type_set_int(network_mysqld_type_t *type, guint64 i, gboolean is_unsigned) {
	if (NULL == type->set_int) return -1;

	return type->set_int(type, i, is_unsigned);
}


int network_mysqld_type_get_double(network_mysqld_type_t *type, double *d) {
	if (NULL == type->get_double) return -1;

	return type->get_double(type, d);
}


int network_mysqld_type_set_double(network_mysqld_type_t *type, double d) {
	if (NULL == type->set_double) return -1;

	return type->set_double(type, d);
}


int network_mysqld_type_get_date(network_mysqld_type_t *type, network_mysqld_type_date_t *date) {
	if (NULL == type->get_date) return -1;

	return type->get_date(type, date);
}


int network_mysqld_type_set_date(network_mysqld_type_t *type, network_mysqld_type_date_t *date) {
	if (NULL == type->set_date) return -1;

	return type->set_date(type, date);
}


int network_mysqld_type_get_time(network_mysqld_type_t *type, network_mysqld_type_time_t *t) {
	if (NULL == type->get_time) return -1;

	return type->get_time(type, t);
}


int network_mysqld_type_set_time(network_mysqld_type_t *type, network_mysqld_type_time_t *t) {
	if (NULL == type->set_time) return -1;

	return type->set_time(type, t);
}


