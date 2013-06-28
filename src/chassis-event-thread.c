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

	g_async_queue_push(chas->threads->event_queue, client_con);

	// choose a event thread
	static guint last_event_thread = 0;
	if (last_event_thread == chas->event_thread_count) last_event_thread = 0;
	chassis_event_thread_t *event_thread = chas->threads->event_threads->pdata[last_event_thread];
	++last_event_thread;

	if (write(event_thread->notify_send_fd, "", 1) != 1) g_error("pipes - write error: %s", g_strerror(errno));
}

static GPrivate tls_index;

void chassis_event_add_self(chassis* chas, struct event* ev, int timeout) {	//工作线程执行，RETRY后将事件重新放入本线程的event_base中
	guint index = GPOINTER_TO_UINT(g_private_get(&tls_index));
	chassis_event_thread_t* thread = g_ptr_array_index(chas->threads->event_threads, index);
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
	chassis_event_thread_t* thread = g_ptr_array_index(chas->threads->event_threads, index);
	struct event_base *event_base = thread->event_base;

	g_assert(event_base); /* the thread-local event-base has to be initialized */

	event_base_set(event_base, ev);
	event_add(ev, NULL);
}

void chassis_event_handle(int G_GNUC_UNUSED event_fd, short G_GNUC_UNUSED events, void* user_data) {
	chassis_event_thread_t* event_thread = user_data;

	char ping[1];
	if (read(event_thread->notify_receive_fd, ping, 1) != 1) g_error("pipes - read error");

	network_mysqld_con* client_con = g_async_queue_try_pop(event_thread->chas->threads->event_queue);
	if (client_con != NULL) network_mysqld_con_handle(-1, 0, client_con);
}

/**
 * create the data structure for a new event-thread
 */
chassis_event_thread_t *chassis_event_thread_new(guint index) {
	chassis_event_thread_t *event_thread;

	event_thread = g_new0(chassis_event_thread_t, 1);

	event_thread->index = index;

	return event_thread;
}

/**
 * free the data-structures for a event-thread
 *
 * joins the event-thread, closes notification-pipe and free's the event-base
 */
void chassis_event_thread_free(chassis_event_thread_t *event_thread) {
	gboolean is_thread = (event_thread->thr != NULL);

	if (!event_thread) return;

	if (event_thread->thr) g_thread_join(event_thread->thr);

	if (event_thread->notify_receive_fd != -1) {
		event_del(&(event_thread->notify_fd_event));
		closesocket(event_thread->notify_receive_fd);
	}
	if (event_thread->notify_send_fd != -1) {
		closesocket(event_thread->notify_send_fd);
	}

	/* we don't want to free the global event-base */
	if (is_thread && event_thread->event_base) event_base_free(event_thread->event_base);

	g_free(event_thread);
}

/**
 * create the event-threads handler
 *
 * provides the event-queue that is contains the event_ops from the event-threads
 * and notifies all the idling event-threads for the new event-ops to process
 */
chassis_event_threads_t *chassis_event_threads_new() {
	chassis_event_threads_t *threads;

	threads = g_new0(chassis_event_threads_t, 1);

	/* create the ping-fds
	 *
	 * the event-thread write a byte to the ping-pipe to trigger a fd-event when
	 * something is available in the event-async-queues
	 */
	threads->event_threads = g_ptr_array_new();
	threads->event_queue = g_async_queue_new();

	return threads;
}

/**
 * free all event-threads
 *
 * frees all the registered event-threads and event-queue
 */
void chassis_event_threads_free(chassis_event_threads_t *threads) {
	guint i;
	network_mysqld_con* con;

	if (!threads) return;

	/* all threads are running, now wait until they are down again */
	for (i = 0; i < threads->event_threads->len; i++) {
		chassis_event_thread_t *event_thread = threads->event_threads->pdata[i];

		chassis_event_thread_free(event_thread);
	}

	g_ptr_array_free(threads->event_threads, TRUE);

	/* free the events that are still in the queue */
	while ((con = g_async_queue_try_pop(threads->event_queue))) {
		network_mysqld_con_free(con);
	}
	g_async_queue_unref(threads->event_queue);
	g_free(threads);
}

/**
 * add a event-thread to the event-threads handler
 */
void chassis_event_threads_add(chassis_event_threads_t *threads, chassis_event_thread_t *thread) {
	g_ptr_array_add(threads->event_threads, thread);
}


/**
 * setup the notification-fd of a event-thread
 *
 * all event-threads listen on the same notification pipe
 *
 * @see chassis_event_handle()
 */ 
int chassis_event_threads_init_thread(chassis_event_threads_t *threads, chassis_event_thread_t *event_thread, chassis *chas) {
	event_thread->event_base = event_base_new();
	event_thread->chas = chas;

    int fds[2];
	if (pipe(fds)) {
		int err;
		err = errno;
		g_error("%s: evutil_socketpair() failed: %s (%d)", 
				G_STRLOC,
				g_strerror(err),
				err);
	}
    event_thread->notify_receive_fd = fds[0];
    event_thread->notify_send_fd = fds[1];

	event_set(&(event_thread->notify_fd_event), event_thread->notify_receive_fd, EV_READ | EV_PERSIST, chassis_event_handle, event_thread);
	event_base_set(event_thread->event_base, &(event_thread->notify_fd_event));
	event_add(&(event_thread->notify_fd_event), NULL);

	return 0;
}

/**
 * event-handler thread
 *
 */
void *chassis_event_thread_loop(chassis_event_thread_t *event_thread) {
	g_private_set(&tls_index, GUINT_TO_POINTER(event_thread->index));
	/**
	 * check once a second if we shall shutdown the proxy
	 */
	while (!chassis_is_shutdown()) {
		struct timeval timeout;
		int r;

		timeout.tv_sec = 1;
		timeout.tv_usec = 0;

		g_assert(event_base_loopexit(event_thread->event_base, &timeout) == 0);

		r = event_base_dispatch(event_thread->event_base);

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
/*
void *chassis_remove_thread_loop(chassis *chas) {
	struct event ev;
	signal_set(&ev, SIGRTMIN, NULL, NULL);
	event_base_set(chas->remove_base, &ev);
	signal_add(&ev, NULL);

	event_base_dispatch(chas->remove_base);
	return NULL;
} 

void chassis_remove_thread_start(chassis *chas) {
	chas->remove_base = event_base_new();
	g_thread_create((GThreadFunc)chassis_remove_thread_loop, chas, TRUE, NULL);
}
*/
/**
 * start all the event-threads 
 *
 * starts all the event-threads that got added by chassis_event_threads_add()
 *
 * @see chassis_event_threads_add
 */
void chassis_event_threads_start(chassis_event_threads_t *threads) {
	guint i;

	g_message("%s: starting %d threads", G_STRLOC, threads->event_threads->len - 1);

	for (i = 1; i < threads->event_threads->len; i++) { /* the 1st is the main-thread and already set up */
		chassis_event_thread_t *event_thread = threads->event_threads->pdata[i];
		GError *gerr = NULL;

		event_thread->thr = g_thread_create((GThreadFunc)chassis_event_thread_loop, event_thread, TRUE, &gerr);

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
