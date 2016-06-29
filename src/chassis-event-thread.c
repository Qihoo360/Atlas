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

#include <glib.h>
#include <errno.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h> /* for write() */
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>	/* for SOCK_STREAM and AF_UNIX/AF_INET */
#endif

#ifdef WIN32
#include <winsock2.h>
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <io.h>	/* for write, read, _pipe etc */
#include <fcntl.h>
#undef WIN32_LEAN_AND_MEAN
#endif

#include <event.h>

#include "chassis-event-thread.h"

#define C(x) x, sizeof(x) - 1
#ifndef WIN32
#define closesocket(x) close(x)
#endif

/**
 * add a event asynchronously
 *
 * the event is added to the global event-queue and a fd-notification is sent allowing any
 * of the event-threads to handle it
 *
 * @see network_mysqld_con_handle()
 */
void chassis_event_add(network_mysqld_con* client_con) {		//主线程执行，ping工作线程，使其初次进入状态机
	chassis* chas = client_con->srv;

	// choose a event thread
	static guint last_thread = 1;
	if (last_thread > chas->event_thread_count) last_thread = 1;
	chassis_event_thread_t *thread = chas->threads->pdata[last_thread];
	++last_thread;

	g_async_queue_push(thread->event_queue, client_con);
	if (write(thread->notify_send_fd, "", 1) != 1) g_error("pipes - write error: %s", g_strerror(errno));
}

static GPrivate tls_index;

void chassis_event_add_self(chassis* chas, struct event* ev, int timeout) {	//工作线程执行，RETRY后将事件重新放入本线程的event_base中
	guint index = GPOINTER_TO_UINT(g_private_get(&tls_index));
	chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, index);
	struct event_base *event_base = thread->event_base;
	event_base_set(event_base, ev);

	if (timeout > 0) {
		struct timeval tm = {timeout, 0};
		event_add(ev, &tm);
	} else {
		event_add(ev, NULL);
	}
}

/**
 * add a event to the current thread 
 *
 * needs event-base stored in the thread local storage
 *
 * @see network_connection_pool_lua_add_connection()
 */
void chassis_event_add_local(chassis *chas, struct event *ev) {		//工作线程或回收线程执行，监听池中连接的超时事件EV_READ，用来销毁超时连接
	guint index = GPOINTER_TO_UINT(g_private_get(&tls_index));
	chassis_event_thread_t* thread = g_ptr_array_index(chas->threads, index);
	struct event_base *event_base = thread->event_base;

	g_assert(event_base); /* the thread-local event-base has to be initialized */

	event_base_set(event_base, ev);
	event_add(ev, NULL);
}

void chassis_event_handle(int G_GNUC_UNUSED event_fd, short G_GNUC_UNUSED events, void* user_data) {
	chassis_event_thread_t* thread = user_data;

	char ping[1];
	if (read(thread->notify_receive_fd, ping, 1) != 1) g_error("pipes - read error");

	network_mysqld_con* client_con = g_async_queue_try_pop(thread->event_queue);
	if (client_con != NULL) network_mysqld_con_handle(-1, 0, client_con);
}

/**
 * create the data structure for a new event-thread
 */
chassis_event_thread_t *chassis_event_thread_new(guint index) {
	chassis_event_thread_t *thread = g_new0(chassis_event_thread_t, 1);

	thread->index = index;

	thread->event_queue = g_async_queue_new();

	return thread;
}

/**
 * free the data-structures for a event-thread
 *
 * joins the event-thread, closes notification-pipe and free's the event-base
 */
void chassis_event_thread_free(chassis_event_thread_t *thread) {
	if (!thread) return;

	if (thread->thr) g_thread_join(thread->thr);

	if (thread->notify_receive_fd != -1) {
		event_del(&(thread->notify_fd_event));
		closesocket(thread->notify_receive_fd);
	}
	if (thread->notify_send_fd != -1) {
		closesocket(thread->notify_send_fd);
	}

	/* we don't want to free the global event-base */
	if (thread->thr != NULL && thread->event_base) event_base_free(thread->event_base);

	network_mysqld_con* con;
	while (con = g_async_queue_try_pop(thread->event_queue)) {
		network_mysqld_con_free(con);
	}
	g_async_queue_unref(thread->event_queue);

	g_free(thread);
}

/**
 * setup the notification-fd of a event-thread
 *
 * all event-threads listen on the same notification pipe
 *
 * @see chassis_event_handle()
 */ 
int chassis_event_threads_init_thread(chassis_event_thread_t *thread, chassis *chas) {
	thread->event_base = event_base_new();
	thread->chas = chas;

	int fds[2];
	if (pipe(fds)) {
		int err;
		err = errno;
		g_error("%s: evutil_socketpair() failed: %s (%d)", 
				G_STRLOC,
				g_strerror(err),
				err);
	}
	thread->notify_receive_fd = fds[0];
	thread->notify_send_fd = fds[1];

	event_set(&(thread->notify_fd_event), thread->notify_receive_fd, EV_READ | EV_PERSIST, chassis_event_handle, thread);
	event_base_set(thread->event_base, &(thread->notify_fd_event));
	event_add(&(thread->notify_fd_event), NULL);

	return 0;
}

/**
 * event-handler thread
 *
 */
void *chassis_event_thread_loop(chassis_event_thread_t *thread) {
	g_private_set(&tls_index, GUINT_TO_POINTER(thread->index));
	/**
	 * check once a second if we shall shutdown the proxy
	 */
	while (!chassis_is_shutdown()) {
		struct timeval timeout;
		int r;

		timeout.tv_sec = 1;
		timeout.tv_usec = 0;

		g_assert(event_base_loopexit(thread->event_base, &timeout) == 0);

		r = event_base_dispatch(thread->event_base);

		if (r == -1) {
#ifdef WIN32
			errno = WSAGetLastError();
#endif
			if (errno == EINTR) continue;
			g_critical("%s: leaving chassis_event_thread_loop early, errno != EINTR was: %s (%d)", G_STRLOC, g_strerror(errno), errno);
			break;
		}
	}

	return NULL;
}

/**
 * start all the event-threads 
 *
 * starts all the event-threads that got added by chassis_event_threads_add()
 *
 * @see chassis_event_threads_add
 */
void chassis_event_threads_start(GPtrArray *threads) {
	guint i;

	g_message("%s: starting %d threads", G_STRLOC, threads->len - 1);

	for (i = 1; i < threads->len; i++) { /* the 1st is the main-thread and already set up */
		chassis_event_thread_t *thread = threads->pdata[i];
		GError *gerr = NULL;

		thread->thr = g_thread_create((GThreadFunc)chassis_event_thread_loop, thread, TRUE, &gerr);

		if (gerr) {
			g_critical("%s: %s", G_STRLOC, gerr->message);
			g_error_free(gerr);
			gerr = NULL;
		}
	}
}

network_connection_pool* chassis_event_thread_pool(network_backend_t* backend) {
	guint index = GPOINTER_TO_UINT(g_private_get(&tls_index));
	return g_ptr_array_index(backend->pools, index);
}
