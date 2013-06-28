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
 

#ifndef _NETWORK_MYSQLD_BINLOG_H_
#define _NETWORK_MYSQLD_BINLOG_H_

#include "network-mysqld-proto.h"

/**
 * stolen from sql/log_event.h
 *
 * (MySQL 5.1.24)
 */
#define ST_SERVER_VER_LEN 50

enum Log_event_type
{
  /*
    Every time you update this enum (when you add a type), you have to
    fix Format_description_log_event::Format_description_log_event().
  */
  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  /*
    NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
    sql_ex, allowing multibyte TERMINATED BY etc; both types share the
    same class (Load_log_event)
  */
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,
  TABLE_MAP_EVENT = 19,

  /*
    These event numbers were used for 5.1.0 to 5.1.15 and are
    therefore obsolete.
   */
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  /*
    These event numbers are used from 5.1.16 and forward
   */
  WRITE_ROWS_EVENT = 23,
  UPDATE_ROWS_EVENT = 24,
  DELETE_ROWS_EVENT = 25,

  /*
    Something out of the ordinary happened on the master
   */
  INCIDENT_EVENT= 26,
  HEARTBEAT_LOG_EVENT= 27,
  IGNORABLE_LOG_EVENT= 28,
  ROWS_QUERY_LOG_EVENT= 29,


  /*
    Add new events here - right above this comment!
    Existing events (except ENUM_END_EVENT) should never change their numbers
  */

  ENUM_END_EVENT /* end marker */
};



/**
 * replication
 */

typedef struct {
	guint64 table_id;

	GString *db_name;
	GString *table_name;

	GPtrArray *fields;
} network_mysqld_table;

NETWORK_API network_mysqld_table *network_mysqld_table_new();
NETWORK_API void network_mysqld_table_free(network_mysqld_table *tbl);
NETWORK_API guint64 *guint64_new(guint64 i);

typedef enum {
	NETWORK_MYSQLD_BINLOG_CHECKSUM_OFF   = 0,
	NETWORK_MYSQLD_BINLOG_CHECKSUM_CRC32 = 1,
	NETWORK_MYSQLD_BINLOG_CHECKSUM_UNDEF = 255
} network_mysqld_binlog_checksum;

typedef struct {
	gchar *filename;

	/* we have to store some information from the format description event 
	 */
	guint header_len;

	/* ... and the table-ids */
	GHashTable *rbr_tables; /* hashed by table-id -> network_mysqld_table */

	network_mysqld_binlog_checksum checksum;
} network_mysqld_binlog;

NETWORK_API network_mysqld_binlog *network_mysqld_binlog_new();
NETWORK_API void network_mysqld_binlog_free(network_mysqld_binlog *binlog);

typedef struct {
	guint32 timestamp;
	enum Log_event_type event_type;
	guint32 server_id;
	guint32 event_size;
	guint32 log_pos;
	guint16 flags;

	union {
		struct {
			guint32 thread_id;
			guint32 exec_time;
			guint8  db_name_len;
			guint16 error_code;

			gchar *db_name;
			gchar *query;
		} query_event;
		struct {
			gchar *binlog_file;
			guint32 binlog_pos;
		} rotate_event;
		struct {
			guint16 binlog_version;
			gchar *master_version;
			guint32 created_ts;
			guint8  log_header_len;
			gchar *perm_events;
			gsize  perm_events_len;
		} format_event;
		struct {
			guint32 name_len;
			gchar *name;

			guint8 is_null;
			guint8 type;
			guint32 charset; /* charset of the string */

			guint32 value_len; 
			gchar *value; /* encoded in binary speak, depends on .type */
		} user_var_event;
		struct {
			guint64 table_id;
			guint16 flags;

			guint8 db_name_len;
			gchar *db_name;
			guint8 table_name_len;
			gchar *table_name;

			guint64 columns_len;
			gchar *columns;

			guint64 metadata_len;
			gchar *metadata;

			guint32 null_bits_len;
			gchar *null_bits;
		} table_map_event;

		struct {
			guint64 table_id;
			guint16 flags;
			
			guint64 columns_len;

			/* before image */
			guint32 used_columns_before_len; /* bytes of the used columns bit-mask */
			gchar *used_columns_before;   /* bit-mask of the columns stored the row */
			guint32 null_bits_before_len; /* bytes used to store the NULL-bits in the row */

			/* after image */
			guint32 used_columns_after_len;
			gchar *used_columns_after;    /* bit-mask of the columns stored the row */
			guint32 null_bits_after_len;  /* bytes used to store the NULL-bits in the row */
			
			guint32 row_len;
			gchar *row;      /* raw row-buffer in the format:
					    [null-bits] [field_0, ...]
					    [null-bits] [field_0, ...]
					    */
		} row_event;

		struct {
			guint8  type;
			guint64 value;
		} intvar;

		struct {
			guint64 xid_id;
		} xid;
		struct {
			guint8 query_len; /* don't use */
			gchar *query;
		} rows_query;
	} event;
} network_mysqld_binlog_event;

NETWORK_API network_mysqld_binlog_event *network_mysqld_binlog_event_new(void);
NETWORK_API void network_mysqld_binlog_event_free(network_mysqld_binlog_event *event);
NETWORK_API int network_mysqld_proto_get_binlog_event_header(network_packet *packet, network_mysqld_binlog_event *event);
NETWORK_API int network_mysqld_proto_get_binlog_event(network_packet *packet, 
		network_mysqld_binlog *binlog,
		network_mysqld_binlog_event *event);
NETWORK_API int network_mysqld_proto_get_binlog_status(network_packet *packet);

typedef struct {
	gchar *binlog_file;
	guint32 binlog_pos;
	guint16 flags;
	guint32 server_id;
} network_mysqld_binlog_dump;

NETWORK_API network_mysqld_binlog_dump *network_mysqld_binlog_dump_new();
NETWORK_API void network_mysqld_binlog_dump_free(network_mysqld_binlog_dump *dump);
NETWORK_API int network_mysqld_proto_append_binlog_dump(GString *packet, network_mysqld_binlog_dump *dump);


NETWORK_API int network_mysqld_binlog_event_tablemap_get(
		network_mysqld_binlog_event *event,
		network_mysqld_table *tbl);

#endif
