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
#include "glib-ext-ref.h"

/**
 * create a new reference object 
 *
 * @see g_ref_unref
 */
GRef *g_ref_new() {
	GRef *ref;

	ref = g_new0(GRef, 1);
	ref->ref_count = 0;
	ref->udata     = NULL;
	
	return ref;
}

/**
 * set the referenced data and its free-function
 *
 * increments the ref-counter by one
 */
void g_ref_set(GRef *ref, gpointer udata, GDestroyNotify udata_free) {
	g_return_if_fail(ref->ref_count == 0);
	
	ref->udata = udata;
	ref->udata_free = udata_free;
	ref->ref_count = 1;
}

/**
 * increment the ref counter 
 */
void g_ref_ref(GRef *ref) {
	g_return_if_fail(ref->ref_count > 0);
	
	ref->ref_count++;
}

/**
 * unreference a object
 *
 * if no other object references this free the object
 */
void g_ref_unref(GRef *ref) {
	if (ref->ref_count == 0) {
		/* not set yet */
	} else if (--ref->ref_count == 0) {
		if (ref->udata_free) {
			ref->udata_free(ref->udata);
			ref->udata = NULL;
		}
		g_free(ref);
	}
}


