/* $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _NETWORK_QUEUE_H_
#define _NETWORK_QUEUE_H_

#include "network-exports.h"

#include <glib.h>

/* a input or output stream */
typedef struct {
	GQueue *chunks;

	size_t len;    /* len in all chunks (w/o the offset) */
	size_t offset; /* offset in the first chunk */
} network_queue;

NETWORK_API network_queue *network_queue_init(void) G_GNUC_DEPRECATED;
NETWORK_API network_queue *network_queue_new(void);
NETWORK_API void network_queue_free(network_queue *queue);
NETWORK_API int network_queue_append(network_queue *queue, GString *chunk);
NETWORK_API GString *network_queue_pop_string(network_queue *queue, gsize steal_len, GString *dest);
NETWORK_API GString *network_queue_peek_string(network_queue *queue, gsize peek_len, GString *dest);

#endif
