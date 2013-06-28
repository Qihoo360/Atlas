/* $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
 

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <glib.h>
#include "chassis-stats.h"

chassis_stats_t *chassis_global_stats = NULL;

chassis_stats_t * chassis_stats_new(void) {
	if (chassis_global_stats != NULL) return chassis_global_stats;
	
	chassis_global_stats = g_new0(chassis_stats_t, 1);
	g_debug("%s: created new global chassis stats at %p", G_STRLOC, (void*)chassis_global_stats);
	
	return chassis_global_stats;
}

void chassis_stats_free(chassis_stats_t *stats) {
	if (!stats) return;
	
	if (stats == chassis_global_stats) {
		g_free(stats);
		chassis_global_stats = NULL;
	} else {
		/* there should only be one glbal chassis stats struct at any given time */
		g_assert_not_reached();
	}
}

GHashTable* chassis_stats_get(chassis_stats_t *stats){
	GHashTable *stats_hash;
	
	if (stats == NULL) return NULL;
	
	/* NOTE: the keys are strdup'ed, the values are simply integers */
	stats_hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

#define STR(x) #x
#define N(x) g_strdup(x)
#define ADD_STAT(x) g_hash_table_insert(stats_hash, N( STR(x)), GUINT_TO_POINTER(g_atomic_int_get(&(stats->x))))
#define ADD_ALLOC_STAT(x) ADD_STAT(x ## _alloc); ADD_STAT(x ## _free);
	
	ADD_ALLOC_STAT(lua_mem);
	ADD_STAT(lua_mem_bytes);
	ADD_STAT(lua_mem_bytes_max);
	
#undef N
#undef STR
#undef ADD_STAT
#undef ADD_ALLOC_STAT
	
	return stats_hash;
}

