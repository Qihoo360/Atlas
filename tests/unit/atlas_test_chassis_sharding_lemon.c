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

#include <glib.h>
#include <string.h>

#include "chassis-sharding.h"
#include <lemon/sqliteInt.h>

void init_mock_sharding_table_rule(sharding_table_t* sharding_table_rule, const gchar* shard_table_name, guint8 shard_type,
		const gchar* shard_key, GArray *shard_dbgroup)
{
	sharding_table_rule->table_name = g_string_new(shard_table_name);
	sharding_table_rule->shard_key = g_string_new(shard_key);
	sharding_table_rule->shard_type = shard_type;
	sharding_table_rule->shard_group = shard_dbgroup;
}

void add_mock_group_range_map(GArray* shard_group, guint64 range_begin, guint64 range_end, guint db_group_index)
{
 	group_range_map_t group_range_map;
	group_range_map.db_group_index = db_group_index;
	group_range_map.range_begin = range_begin;
	group_range_map.range_end = range_end;

	g_array_append_val(shard_group, group_range_map);
}

void test_sharding_get_dbgroups_by_range_select() {
	GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "SELECT * FROM test_shard WHERE id = 1;";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));

    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id= 99;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 200;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 10000;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_WRONG_RANGE);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE nonshard_key = 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_NO_SHARDKEY);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1, 150);";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1, 50) AND id = 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
	g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1, 50) OR id = 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1, 50) OR id > 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(3, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1, 1);";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1);";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    
    /**
     * BETWEEN
     */
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 OR 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg != NULL);
    g_free(errMsg);
    errMsg = NULL;

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 200 AND 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id NOT BETWEEN 200 AND 150;"; // hit all
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 200 AND 200;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 200 AND 350;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1000 AND 2000;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_WRONG_RANGE);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 299;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 99 OR id = 150;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 99 OR id >= 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    /**
     * range sql
     */
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 50 AND id < 250;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    // auto makeup range, 100 - 150
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 OR (id < 150 AND id > 100)";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 OR (id < 150 OR id > 100)";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 50 AND id <= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    /**
     * shrink the range
     */
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 50 AND id < 200"; // equal to id < 50
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id < 100"; 
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 50 AND id < 200 OR id > 250"; 
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 50"; 
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 99"; 
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 150 OR id = 50"; 
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE test_shard.id > 150 OR test_shard.id = 50"; 
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE test_shard.id BETWEEN 1 AND 99 OR id >= 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard, nosharding WHERE test_shard.id = nosharding.id";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_NO_SHARDKEY);
exit:
    sqlite3ParseDelete(parse_obj);

    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);
}

void test_sharding_get_dbgroups_by_range_delete() {
    GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "DELETE FROM test_shard WHERE id = 99;";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "DELETE FROM test_shard WHERE id <= 99;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "DELETE FROM test_shard WHERE id > 99;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "DELETE FROM test_shard WHERE id BETWEEN 0 AND 150;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "DELETE FROM test_shard WHERE id >= 0 AND id <= 150;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "DELETE FROM test_shard WHERE id >= 0 AND id <= 99 AND age = 200 AND name = 'test';";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "DELETE FROM test_shard WHERE id = 1 OR id = 150";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
exit:
    sqlite3ParseDelete(parse_obj);
    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);

}

void test_sharding_get_dbgroups_by_range_insert() {
    GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "INSERT INTO test_shard(id, other) VALUES(1, 0);;";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "REPLACE INTO test_shard(id, other) VALUES(1, 0);";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "REPLACE INTO test_shard VALUES(1, 0);";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "INSERT INTO test_shard VALUES(1, 0), (150, 0);";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "INSERT INTO test_shard VALUES(1, 0), (2, 0);";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "INSERT INTO test_shard VALUES('test', 0);";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "INSERT INTO test_shard SET name = 'test';";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "INSERT INTO test_shard SET id = 1, name = 'test';";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "INSERT INTO `yp_xiangce`.`xiangce_idgen` () VALUES ()";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(SHARDING_RET_ERR_HIT_NOTHING, ==, ret);
exit:
    sqlite3ParseDelete(parse_obj);
    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);
}

void test_sharding_get_dbgroups_by_range_update() {
    GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "UPDATE test_shard SET other = 0";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "UPDATE test_shard SET other = 0 WHERE id = 1";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "UPDATE test_shard SET other = 0 WHERE id IN (1, 200)";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "UPDATE test_shard SET other = 0 WHERE id IN (1, 2)";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "UPDATE test_shard SET other = 0 WHERE id IN (1)";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
exit:
    sqlite3ParseDelete(parse_obj);
    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);
}

void add_mock_group_hash_map(GArray* shard_group, guint db_group_index)
{
 	group_hash_map_t group_hash_map;
	group_hash_map.db_group_index = db_group_index;

	g_array_append_val(shard_group, group_hash_map);
}

void test_sharding_get_dbgroups_by_hash() {
    GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_hash_map_t));
	add_mock_group_hash_map(shard_group, 0);
	add_mock_group_hash_map(shard_group, 1);
	add_mock_group_hash_map(shard_group, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_HASH, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "SELECT * FROM test_shard WHERE id = 1;";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 1;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 10;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 and id > 20;";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (11, 21);";
    errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

exit:
    sqlite3ParseDelete(parse_obj);
    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);

}

void test_sharding_is_support_sql() {
    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "SELECT GET_LOCK('20090529', 60);";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert_cmpint(FALSE, ==, sharding_is_support_sql(parse_obj));
exit:
    sqlite3ParseDelete(parse_obj);
}

void test_where_not_syntax_sql() {
	GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "SELECT * FROM test_shard WHERE NOT id = 1;";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 1 OR id = 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id != 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 1 AND id != 2;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 OR id != 2;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT NOT id != 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT NOT id != 99 AND id >= 99;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id <= 99 AND id < 199;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id > 99 OR NOT id < 200;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id IN (1, 50);";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id IN (1, 50) AND id != 10";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id IN (1, 50) AND id != 10";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id IN (1, 50) OR id != 10";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id >= 99 AND id <= 199 AND id != 99";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 OR id <= 100 AND id != 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id != 100 AND id != 101;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "select * from nosharding_test where id < 100 AND id NOT BETWEEN 150 AND 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND NOT id IN (100, 101, 150);";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 50 AND NOT id IN (100, 101, 150);";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE NOT id IN (100, 101, 150) AND id <= 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
exit:
    sqlite3ParseDelete(parse_obj);
    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);
}

void test_where_merge_sql() {
	GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);

    Parse *parse_obj = sqlite3ParseNew();
    char *errMsg = 0;
    char *sql = "SELECT * FROM test_shard WHERE id = 1 AND id = 2;";
    int errNum = sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_t parse_info;
    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id = 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id != 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id != 2;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 150 AND id > 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id > 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id >= 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id >= 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id < 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id < 200;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id <= 100;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id <= 99;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id <= 99;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 1 AND id > 99 AND id <= 199;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 99 AND id <= 199 AND id = 1;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id > 99 AND id <= 199;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND id BETWEEN 100 AND 250;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 100 AND NOT id BETWEEN 100 AND 250;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id = 99 AND id BETWEEN 100 AND 250;";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 99 AND id != 99";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 99 AND id != 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 100 AND id != 99";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 100 AND id > 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 199 AND id >= 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 199 AND id < 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 100 AND id <= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 99 AND id BETWEEN 99 AND 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 99 AND id >= 99 AND id <= 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id != 200 AND id >= 100 AND id <= 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE age = 20 AND birthday = '1990/7/12' AND id >= 100 AND id <= 200 AND name = 'test' AND id != 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id = 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id != 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id > 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id > 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id >= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id >= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id < 299";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id <= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id <= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 200 AND id BETWEEN 100 AND 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 200 AND id >= 100 AND id <= 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id < 100 AND id >= 100 AND id <= 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    
    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id = 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id = 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id != 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id != 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id > 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id > 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id >= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id < 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 100 AND id <= 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 200 AND id BETWEEN 200 AND 1000";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id <= 200 AND id BETWEEN 300 AND 1000";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 AND id = 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 AND nonshardkey = 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE name = 'test' AND id > 200 AND nonshardkey = 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE name = 'test' AND nonshardkey = 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_NO_SHARDKEY);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 AND id != 1";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 AND id > 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 AND id < 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 200 AND id <= 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 10 AND id < 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 10 AND id BETWEEN 100 AND 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id > 10 AND id NOT BETWEEN 100 AND 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 99 AND id BETWEEN 100 AND 199";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 99 AND id > 100";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_HIT_NOTHING);
    g_assert_cmpint(0, ==, hit_db_groups->len);

    sqlite3ParseReset(parse_obj);
    g_array_set_size(hit_db_groups, 0);
    sql = "SELECT * FROM test_shard WHERE id BETWEEN 1 AND 99 AND id < 200";
    sqlite3RunParser1(parse_obj, sql, strlen(sql), &errMsg);
    g_assert(errMsg == NULL);

    parse_info_init(&parse_info, parse_obj, sql, strlen(sql));
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, &parse_info);
    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));

exit:
    sqlite3ParseDelete(parse_obj);
    g_array_free(shard_group, TRUE);
    g_array_free(hit_db_groups, TRUE);
}

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

    g_test_add_func("/core/test_sharding_get_dbgroups_by_range_select", test_sharding_get_dbgroups_by_range_select);
    g_test_add_func("/core/test_sharding_get_dbgroups_by_range_delete", test_sharding_get_dbgroups_by_range_delete);
    g_test_add_func("/core/test_sharding_get_dbgroups_by_range_insert", test_sharding_get_dbgroups_by_range_insert);
    g_test_add_func("/core/test_sharding_get_dbgroups_by_range_update", test_sharding_get_dbgroups_by_range_update);
    g_test_add_func("/core/test_sharding_get_dbgroups_by_hash", test_sharding_get_dbgroups_by_hash);
    g_test_add_func("/core/test_sharding_is_support_sql", test_sharding_is_support_sql);
    g_test_add_func("/core/test_where_not_syntax_sql", test_where_not_syntax_sql);
    g_test_add_func("/core/test_where_merge_sql", test_where_merge_sql);
	return g_test_run();
}
