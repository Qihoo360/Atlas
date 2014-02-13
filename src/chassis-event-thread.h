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
 

#ifndef _CHASSIS_EVENT_THREAD_H_
#define _CHASSIS_EVENT_THREAD_H_

#include <glib.h>    /* GPtrArray */

#include "chassis-exports.h"
#include "chassis-mainloop.h"
#include "network-backend.h"
#include "network-mysqld.h"

CHASSIS_API void chassis_event_add(network_mysqld_con *client_con);
CHASSIS_API void chassis_event_add_self(chassis *chas, struct event *ev, int timeout);
CHASSIS_API void chassis_event_add_local(chassis *chas, struct event *ev);

/**
 * a event-thread
 */
typedef struct {
	chassis *chas;

	//int notify_fd;
	int notify_receive_fd;
	int notify_send_fd;

	struct event notify_fd_event;

	GThread *thr;

	struct event_base *event_base;

	guint index;

	GAsyncQueue *event_queue;
} chassis_event_thread_t;

CHASSIS_API chassis_event_thread_t *chassis_event_thread_new();
CHASSIS_API void chassis_event_thread_free(chassis_event_thread_t *thread);
CHASSIS_API void chassis_event_handle(int event_fd, short events, void *user_data);
CHASSIS_API void chassis_event_thread_set_event_base(chassis_event_thread_t *thread, struct event_base *event_base);
CHASSIS_API void *chassis_event_thread_loop(chassis_event_thread_t *thread);

CHASSIS_API int chassis_event_threads_init_thread(chassis_event_thread_t *thread, chassis *chas);
CHASSIS_API void chassis_event_threads_start(GPtrArray *threads);

CHASSIS_API network_connection_pool* chassis_event_thread_pool(network_backend_t* backend);

#endif
