/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

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

#include "glib-ext.h"
#include "sys-pedantic.h"
#include <string.h>

/** @file
 * helper functions for common glib operations
 *
 * - g_list_string_free()
 */

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

/**
 * free function for GStrings in a GList
 */
void g_list_string_free(gpointer data, gpointer UNUSED_PARAM(user_data)) {
	g_string_free(data, TRUE);
}

/**
 * free function for GStrings in a GHashTable
 */
void g_hash_table_string_free(gpointer data) {
	g_string_free(data, TRUE);
}

/**
 * hash function for GString
 */
guint g_hash_table_string_hash(gconstpointer _key) {
	return g_string_hash(_key);
}

/**
 * compare function for GString
 */
gboolean g_hash_table_string_equal(gconstpointer _a, gconstpointer _b) {
	return g_string_equal(_a, _b);
}

/**
 * true-function for g_hash_table_foreach
 */
gboolean g_hash_table_true(gpointer UNUSED_PARAM(key), gpointer UNUSED_PARAM(value), gpointer UNUSED_PARAM(u)) {
	return TRUE;
}	

gpointer g_hash_table_lookup_const(GHashTable *h, const gchar *name, gsize name_len) {
	GString key;

	key.str = (char *)name; /* we are still const */
	key.len = name_len;

	return g_hash_table_lookup(h, &key);
}

/**
 * hash function for case-insensitive strings 
 */
guint g_istr_hash(gconstpointer v) {
	/* djb2 */
	const unsigned char *p = v;
	unsigned char c;
	guint32 h = 5381;
	
	while ((c = *p++)) {
		h = ((h << 5) + h) + g_ascii_toupper(c);
	}
	
	return h;
}

/**
 * duplicate a GString
 */
GString *g_string_dup(GString *src) {
	GString *dst = g_string_sized_new(src->len);

	g_string_assign(dst, src->str);

	return dst;
}

/**
 * compare two strings (gchar arrays), whose lengths are known
 */
gboolean strleq(const gchar *a, gsize a_len, const gchar *b, gsize b_len) {
	if (a_len != b_len) return FALSE;
	return (0 == memcmp(a, b, a_len));
}

int g_string_get_time(GString *s, GTimeVal *gt) {
	time_t t = gt->tv_sec;

#ifndef HAVE_GMTIME_R
	static GStaticMutex m = G_STATIC_MUTEX_INIT; /* gmtime() isn't thread-safe */	/*remove lock*/

	g_static_mutex_lock(&m);	/*remove lock*/

	s->len = strftime(s->str, s->allocated_len, "%Y-%m-%dT%H:%M:%S.", gmtime(&(t)));
	
	g_static_mutex_unlock(&m);	/*remove lock*/
#else
	struct tm tm;
	gmtime_r(&(t), &tm);
	s->len = strftime(s->str, s->allocated_len, "%Y-%m-%dT%H:%M:%S.", &tm);
#endif

	/* append microsec + Z */
	g_string_append_printf(s, "%03ldZ", gt->tv_usec / 1000);
	
	return 0;
}

int g_string_get_current_time(GString *s) {
	GTimeVal gt;

	g_get_current_time(&gt);

	return g_string_get_time(s, &gt);
}

/**
 * calculate the difference between two GTimeVal values, in usec
 * positive return value in *tdiff means *told is indeed "earlier" than *tnew,
 * negative return value means the reverse
 * Caller is responsible for passing valid pointers
 */
void ge_gtimeval_diff(GTimeVal *told, GTimeVal *tnew, gint64 *tdiff) {
	*tdiff = (gint64) tnew->tv_sec - told->tv_sec;
	*tdiff *= G_USEC_PER_SEC;
	*tdiff += (gint64) tnew->tv_usec - told->tv_usec;
}

GString * g_string_assign_len(GString *s, const char *str, gsize str_len) {
	g_string_truncate(s, 0);
	return g_string_append_len(s, str, str_len);
}

void g_debug_hexdump(const char *msg, const void *_s, size_t len) {
	GString *hex;
	size_t i;
	const unsigned char *s = _s;
		
       	hex = g_string_new(NULL);

	for (i = 0; i < len; i++) {
		if (i % 16 == 0) {
			g_string_append_printf(hex, "[%04"G_GSIZE_MODIFIER"x]  ", i);
		}
		g_string_append_printf(hex, "%02x", s[i]);

		if ((i + 1) % 16 == 0) {
			size_t j;
			g_string_append_len(hex, C("  "));
			for (j = i - 15; j <= i; j++) {
				g_string_append_c(hex, g_ascii_isprint(s[j]) ? s[j] : '.');
			}
			g_string_append_len(hex, C("\n  "));
		} else {
			g_string_append_c(hex, ' ');
		}
	}

	if (i % 16 != 0) {
		/* fill up the line */
		size_t j;

		for (j = 0; j < 16 - (i % 16); j++) {
			g_string_append_len(hex, C("   "));
		}

		g_string_append_len(hex, C(" "));
		for (j = i - (len % 16); j < i; j++) {
			g_string_append_c(hex, g_ascii_isprint(s[j]) ? s[j] : '.');
		}
	}

	g_debug("(%s) %"G_GSIZE_FORMAT" bytes:\n  %s", 
			msg, 
			len,
			hex->str);

	g_string_free(hex, TRUE);
}

/**
 * compare two GStrings for case-insensitive equality using UTF8
 */
gboolean g_string_equal_ci(const GString *a, const GString *b) {
	char *a_ci, *b_ci;
	gsize a_ci_len, b_ci_len;
	gboolean is_equal = FALSE;

	if (g_string_equal(a, b)) return TRUE;

	a_ci = g_utf8_casefold(S(a));
	a_ci_len = strlen(a_ci);
	
	b_ci = g_utf8_casefold(S(b));
	b_ci_len = strlen(b_ci);

	is_equal = strleq(a_ci, a_ci_len, b_ci, b_ci_len);

	g_free(a_ci);
	g_free(b_ci);

	return is_equal;
}

/**
 * compare two memory records for equality
 */
gboolean g_memeq(const char *a, gsize a_len, const char *b, gsize b_len) {
	if (a_len != b_len) return FALSE;

	return (0 == memcmp(a, b, b_len));
}


