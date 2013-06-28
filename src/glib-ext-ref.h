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
 

#ifndef _GLIB_EXT_REF_H_
#define _GLIB_EXT_REF_H_

#include <glib.h>

#include "chassis-exports.h"

/**
 * a ref-counted c-structure
 *
 */
typedef struct {
	gpointer udata;
	GDestroyNotify udata_free;

	gint ref_count;
} GRef;

CHASSIS_API GRef *g_ref_new(void);
CHASSIS_API void g_ref_set(GRef *ref, gpointer udata, GDestroyNotify udata_free);
CHASSIS_API void g_ref_ref(GRef *ref);
CHASSIS_API void g_ref_unref(GRef *ref);

#endif
