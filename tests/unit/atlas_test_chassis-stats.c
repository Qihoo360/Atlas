/**
 * Author: gowink
 * email: golangwink@gmail.com
 */
#include <glib.h>
#include "chassis-stats.h"

void test_chassis_stats_new() {
	chassis_stats_t *chassis_stats_obj = chassis_stats_new();
	g_assert(chassis_stats_obj != NULL);

	chassis_stats_free(chassis_stats_obj);
}

void test_chassis_stats_get() {
	chassis_stats_t *chassis_stats_obj = chassis_stats_new();
	g_assert(chassis_stats_obj != NULL);

	chassis_stats_obj->lua_mem_alloc = 1;
	chassis_stats_obj->lua_mem_free = 2;
	chassis_stats_obj->lua_mem_bytes = 3;
	chassis_stats_obj->lua_mem_bytes_max = 4;
		
	GHashTable* hashtable = chassis_stats_get(chassis_stats_obj);
	
	gint lua_mem_alloc = GPOINTER_TO_INT(g_hash_table_lookup(hashtable, "lua_mem_alloc"));
	g_assert_cmpint(lua_mem_alloc, ==, 1);

	gint lua_mem_free = GPOINTER_TO_INT(g_hash_table_lookup(hashtable, "lua_mem_free"));
	g_assert_cmpint(lua_mem_free, ==, 2);

	gint lua_mem_bytes = GPOINTER_TO_INT(g_hash_table_lookup(hashtable, "lua_mem_bytes"));
	g_assert_cmpint(lua_mem_bytes, ==, 3);

	gint lua_mem_bytes_max = GPOINTER_TO_INT(g_hash_table_lookup(hashtable, "lua_mem_bytes_max"));
	g_assert_cmpint(lua_mem_bytes_max, ==, 4);

	chassis_stats_free(chassis_stats_obj);
}

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/test_chassis_stats_new", test_chassis_stats_new);
	g_test_add_func("/core/test_chassis_stats_get", test_chassis_stats_get);
	return g_test_run();
}	
