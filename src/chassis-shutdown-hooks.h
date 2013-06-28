/* $%BEGINLICENSE%$
 Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _CHASSIS_SHUTDOWN_HOOKS_H_
#define _CHASSIS_SHUTDOWN_HOOKS_H_

#include <glib.h>    /* GPtrArray */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "chassis-exports.h"

typedef struct {
	void (*func)(gpointer _udata);
	gpointer udata;
	gboolean is_called;
} chassis_shutdown_hook_t;

CHASSIS_API chassis_shutdown_hook_t *chassis_shutdown_hook_new(void);
CHASSIS_API void chassis_shutdown_hook_free(chassis_shutdown_hook_t *);

typedef struct {
	GMutex *mutex;
	GHashTable *hooks;
} chassis_shutdown_hooks_t;

CHASSIS_API chassis_shutdown_hooks_t *chassis_shutdown_hooks_new(void);
CHASSIS_API void chassis_shutdown_hooks_free(chassis_shutdown_hooks_t *);
CHASSIS_API gboolean chassis_shutdown_hooks_register(chassis_shutdown_hooks_t *hooks,
		const char *key, gsize key_len,
		chassis_shutdown_hook_t *hook);
CHASSIS_API void chassis_shutdown_hooks_call(chassis_shutdown_hooks_t *hooks);

#endif
