/* $%BEGINLICENSE%$
   Copyright (c) 2012, 2015, Qihoo 360 and/or its affiliates. All rights reserved.

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
#include "glib-ext.h"
#include <stdio.h>

gint guint_compare_func(gconstpointer value1, gconstpointer value2) {

    if (*((guint*)value1) > *((guint*)value2)) {
        return 1;
    } else if (*((guint*)value1) == *((guint*)value2)) {
        return 0;
    } else {
        return -1;
    }
}

void test_g_array_unique_append_val() {
    GArray* array = g_array_new(FALSE, TRUE, sizeof(guint));
    guint value = 10;
    g_array_unique_append_val(array, &value, guint_compare_func);
    g_assert_cmpint(1, ==, array->len);
    
    g_array_unique_append_val(array, &value, guint_compare_func);
    g_assert_cmpint(1, ==, array->len);

    guint i;
    for (i = 0; i < 500; ++i) {
        value = g_random_int_range(0, 50);
        g_array_unique_append_val(array, &value, guint_compare_func);
    }

    GHashTable* duplicate_examiner = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, g_free);
    for (i = 0; i < array->len; ++i) {
        guint *key = g_new(guint, 1);
        guint *value = g_new(guint, 1);
        *key = *value = g_array_index(array, guint, i);
        gpointer find_value = g_hash_table_lookup(duplicate_examiner, key);
        g_assert_cmpint(NULL, ==, find_value);
        g_hash_table_insert(duplicate_examiner, key, value);
    }
    g_hash_table_destroy(duplicate_examiner);
}

int main(int argc, char **argv) {
    g_test_init(&argc, &argv, NULL);
    g_test_bug_base("http://bugs.mysql.com/");

    g_test_add_func("/core/test_g_array_unique_append_val", test_g_array_unique_append_val);

    return g_test_run();
}
