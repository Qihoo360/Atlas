#ifndef __NETWORK_MYSQLD_TYPE_H__
#define __NETWORK_MYSQLD_TYPE_H__

#ifdef _WIN32
/* mysql.h needs SOCKET defined */
#include <winsock2.h>
#endif
#include <mysql.h>
#include <glib.h>

#include "network-mysqld-proto.h"

#include "network-exports.h"

/**
 * struct for the MYSQL_TYPE_DATE and friends
 */
typedef struct {
	guint16 year;
	guint8  month;
	guint8  day;
	
	guint8  hour;
	guint8  min;
	guint8  sec;

	guint32 nsec; /* the nano-second part */
} network_mysqld_type_date_t;

#define NETWORK_MYSQLD_TYPE_DATE_MIN_BUF_LEN (sizeof("2010-10-27"))
#define NETWORK_MYSQLD_TYPE_DATETIME_MIN_BUF_LEN (sizeof("2010-10-27 19:27:30.000000001"))
#define NETWORK_MYSQLD_TYPE_TIMESTAMP_MIN_BUF_LEN NETWORK_MYSQLD_TYPE_DATETIME_MIN_BUF_LEN

/**
 * struct for the MYSQL_TYPE_TIME 
 */
typedef struct {
	guint8  sign;
	guint32 days;
	
	guint8  hour;
	guint8  min;
	guint8  sec;

	guint32 nsec; /* the nano-second part */
} network_mysqld_type_time_t;

#define NETWORK_MYSQLD_TYPE_TIME_MIN_BUF_LEN (sizeof("-2147483647 19:27:30.000000001"))

typedef struct _network_mysqld_type_t network_mysqld_type_t;

struct _network_mysqld_type_t {
	enum enum_field_types type;

	gpointer data;
	void (*free_data)(network_mysqld_type_t *type);

	/**
	 * get a copy of ->data as GString 
	 *
	 * @param type the type to get the data from
	 * @param s    GString that the converted data will be assigned too
	 * @return 0 on success, -1 on error
	 */
	int (*get_gstring)(network_mysqld_type_t *type, GString *s);
	/**
	 * expose the ->data as constant string 
	 *
	 * only available for types that have a "string" storage like _STRING, _CHAR, _BLOB
	 * the caller can copy the data out, but not change it
	 *
	 * @param type the type to get the data from
	 * @param s    place to store the pointer to the const char * in
	 * @param s_len length of the const char *
	 * @return 0 on success, -1 on error
	 */
	int (*get_string_const)(network_mysqld_type_t *type, const char **s, gsize *s_len);
	/**
	 * get a copy of ->data as char *
	 *
	 * has 2 modes:
	 * - no-alloc-mode if *s is not NULL where it is expected that s and s_len point
	 *   to a buffer of that size that we can copy into
	 *   *s_len will contain the size of the stored string on success
	 *   if *s_len is too small, -1 will be returned
	 * - alloc-mode when *s is NULL where we return a alloced buffer that is large enough
	 *
	 * @param type the type to get the data from
	 * @param s    pointer to a buffer of *s_len size or pointer to (char *)NULL for alloc-mode
	 * @param s_len pointer to the length of the buffer if *s is not NULL. Points to the length of the *s on success
	 * @return 0 on success, -1 on error
	 */
	int (*get_string)(network_mysqld_type_t *type, char **s, gsize *len);
	/**
	 * set the ->data from a string 
	 */
	int (*set_string)(network_mysqld_type_t *type, const char *s, gsize s_len);
	/**
	 * get ->data as uint64
	 */
	int (*get_int)(network_mysqld_type_t *type, guint64 *i, gboolean *is_unsigned);
	/**
	 * set ->data from uint64
	 */
	int (*set_int)(network_mysqld_type_t *type, guint64 i, gboolean is_unsigned);
	/**
	 * get ->data as double
	 */
	int (*get_double)(network_mysqld_type_t *type, double *d);
	/**
	 * set ->data from double
	 */
	int (*set_double)(network_mysqld_type_t *type, double d);
	int (*get_date)(network_mysqld_type_t *type, network_mysqld_type_date_t *date);
	int (*set_date)(network_mysqld_type_t *type, network_mysqld_type_date_t *date);
	/**
	 * get the ->data as _time_t 
	 */
	int (*get_time)(network_mysqld_type_t *type, network_mysqld_type_time_t *t);
	/**
	 * set the ->data from a _time_t
	 */
	int (*set_time)(network_mysqld_type_t *type, network_mysqld_type_time_t *t);


	gboolean is_null;     /**< is the value of this type NULL */
	gboolean is_unsigned; /**< is the type signed or unsigned, only used by the integer types */
}; 


NETWORK_API network_mysqld_type_t *network_mysqld_type_new(enum enum_field_types _type);
NETWORK_API void network_mysqld_type_free(network_mysqld_type_t *type);

/**
 * wrappers around the gettors and settors 
 *
 * @return -1 if no settor or gettor defined or settor or gettor failed to convert
 */
NETWORK_API int network_mysqld_type_get_gstring(network_mysqld_type_t *type, GString *s);
NETWORK_API int network_mysqld_type_get_gstring(network_mysqld_type_t *type, GString *s);
NETWORK_API int network_mysqld_type_get_string_const(network_mysqld_type_t *type, const char **s, gsize *s_len);
NETWORK_API int network_mysqld_type_get_string(network_mysqld_type_t *type, char **s, gsize *len);
NETWORK_API int network_mysqld_type_set_string(network_mysqld_type_t *type, const char *s, gsize s_len);
NETWORK_API int network_mysqld_type_get_int(network_mysqld_type_t *type, guint64 *i, gboolean *is_unsigned);
NETWORK_API int network_mysqld_type_set_int(network_mysqld_type_t *type, guint64 i, gboolean is_unsigned);
NETWORK_API int network_mysqld_type_get_double(network_mysqld_type_t *type, double *d);
NETWORK_API int network_mysqld_type_set_double(network_mysqld_type_t *type, double d);
NETWORK_API int network_mysqld_type_get_date(network_mysqld_type_t *type, network_mysqld_type_date_t *date);
NETWORK_API int network_mysqld_type_set_date(network_mysqld_type_t *type, network_mysqld_type_date_t *date);
NETWORK_API int network_mysqld_type_get_time(network_mysqld_type_t *type, network_mysqld_type_time_t *t);
NETWORK_API int network_mysqld_type_set_time(network_mysqld_type_t *type, network_mysqld_type_time_t *t);

#endif
