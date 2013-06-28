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
 

#ifndef _QUERY_HANDLING_H_
#define _QUERY_HANDLING_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <glib.h>

#include "network-exports.h"

typedef struct {
	/**
	 * the content of the OK packet 
	 */
	guint16 server_status;
	guint16 warning_count;
	guint64 affected_rows;
	guint64 insert_id;
    
	gboolean was_resultset;                      /**< if set, affected_rows and insert_id are ignored */
	
	gboolean binary_encoded;                     /**< if set, the row data is binary encoded. we need the metadata to decode */
    
	/**
	 * MYSQLD_PACKET_OK or MYSQLD_PACKET_ERR
	 */	
	guint8 query_status;
} query_status;

typedef struct {
	GString *query;
    
	int id;                                 /**< a unique id set by the scripts to map the query to a handler */
    
	/* the userdata's need them */
	GQueue *result_queue;                   /**< the data to parse */
	query_status qstat;                     /**< summary information about the query status */
    
	guint64 ts_read_query;                  /**< microsec timestamp when we added this query to the queues */
	guint64 ts_read_query_result_first;     /**< microsec timestamp when we received the first packet */
	guint64 ts_read_query_result_last;      /**< microsec timestamp when we received the last packet */

	guint64      rows;
	guint64      bytes;

	gboolean     resultset_is_needed;       /**< flag to announce if we have to buffer the result for later processing */
} injection;

/**
 * a injection_queue is GQueue for now
 *
 */
typedef GQueue network_injection_queue;

NETWORK_API network_injection_queue *network_injection_queue_new(void);
NETWORK_API void network_injection_queue_free(network_injection_queue *q);
NETWORK_API void network_injection_queue_reset(network_injection_queue *q);
NETWORK_API void network_injection_queue_prepend(network_injection_queue *q, injection *inj);
NETWORK_API void network_injection_queue_append(network_injection_queue *q, injection *inj);
NETWORK_API guint network_injection_queue_len(network_injection_queue *q);

/**
 * parsed result set
 */
typedef struct {
	GQueue *result_queue;   /**< where the packets are read from */
    
	GPtrArray *fields;      /**< the parsed fields */
    
	GList *rows_chunk_head; /**< pointer to the EOF packet after the fields */
	GList *row;             /**< the current row */
    
	query_status qstat;     /**< state of this query */
	
	guint64      rows;
	guint64      bytes;
} proxy_resultset_t;

NETWORK_API injection *injection_new(int id, GString *query);
NETWORK_API void injection_free(injection *i);

NETWORK_API proxy_resultset_t *proxy_resultset_init() G_GNUC_DEPRECATED;
NETWORK_API proxy_resultset_t *proxy_resultset_new();
NETWORK_API void proxy_resultset_free(proxy_resultset_t *res);

#endif /* _QUERY_HANDLING_H_ */
