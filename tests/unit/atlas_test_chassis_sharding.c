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
#include "lib/sql-tokenizer.h"

#define C(x) x, sizeof(x) - 1

guint get_table_index(GPtrArray* tokens, gint* d, gint* t) {
	*d = *t = -1;

	sql_token** ts = (sql_token**)(tokens->pdata);
	guint len = tokens->len;

	guint i = 1, j;
	while (ts[i]->token_id == TK_COMMENT && ++i < len);
	sql_token_id token_id = ts[i]->token_id;

	if (token_id == TK_SQL_SELECT || token_id == TK_SQL_DELETE) {
		for (; i < len; ++i) {
			if (ts[i]->token_id == TK_SQL_FROM) {
				for (j = i+1; j < len; ++j) {
					if (ts[j]->token_id == TK_SQL_WHERE) break;

					if (ts[j]->token_id == TK_LITERAL) {
						if (j + 2 < len && ts[j+1]->token_id == TK_DOT) {
							*d = j;
							*t = j + 2;
						} else {
							*t = j;
						}
						break;
					}
				}
				break;
			}
		}
		return 1;

	} else if (token_id == TK_SQL_UPDATE) {
		for (; i < len; ++i) {
			if (ts[i]->token_id == TK_SQL_SET) break;

			if (ts[i]->token_id == TK_LITERAL) {
				if (i + 2 < len && ts[i+1]->token_id == TK_DOT) {
					*d = i;
					*t = i + 2;

				} else {
					*t = i;
				}
				break;
			}
		}

		return 2;
	} else if (token_id == TK_SQL_INSERT || token_id == TK_SQL_REPLACE) {
		for (; i < len; ++i) {
			gchar* str = ts[i]->text->str;
			if (strcasecmp(str, "VALUES") == 0 || strcasecmp(str, "VALUE") == 0) break;

			sql_token_id token_id = ts[i]->token_id;
			if (token_id == TK_LITERAL && i + 2 < len && ts[i+1]->token_id == TK_DOT) {
				*d = i;
				*t = i + 2;
				break;

			} else if (token_id == TK_LITERAL || token_id == TK_FUNCTION) {
				if (i == len - 1) {
					*t = i;
					break;

				} else {
					str = ts[i+1]->text->str;
					token_id = ts[i+1]->token_id;
					if (strcasecmp(str, "VALUES") == 0 || strcasecmp(str, "VALUE") == 0 || token_id == TK_OBRACE || token_id == TK_SQL_SET) {
						*t = i;
						break;
					}
				}
			}
		}
		return 3;
	}
	return 0;
}

void init_mock_sharding_table_rule(sharding_table_t* sharding_table_rule, const gchar* shard_table_name, guint8 shard_type,
		const gchar* shard_key, GArray *shard_dbgroup)
{
	sharding_table_rule->table_name = g_string_new(shard_table_name);
	sharding_table_rule->shard_key = g_string_new(shard_key);
	sharding_table_rule->shard_type = shard_type;
	sharding_table_rule->shard_group = shard_dbgroup;
}

void clear_mock_sharding_table_rule(sharding_table_t* sharding_table_rule)
{
	g_string_free(sharding_table_rule->table_name, TRUE);
	g_string_free(sharding_table_rule->shard_key, TRUE);
}

void add_mock_dbgroup(GArray *dbgroups, int groupid)
{
	db_group_t dbgroup;
	dbgroup.group_id = groupid;
	g_array_append_val(dbgroups, dbgroup);
}

void add_mock_group_range_map(GArray* shard_group, guint64 range_begin, guint64 range_end, guint db_group_index)
{
 	group_range_map_t group_range_map;
	group_range_map.db_group_index = db_group_index;
	group_range_map.range_begin = range_begin;
	group_range_map.range_end = range_end;

	g_array_append_val(shard_group, group_range_map);
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

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard_hash", SHARDING_TYPE_HASH, "id", shard_group);

	/**
	 *  select
	 */
	GPtrArray *sql_tokens = sql_tokens_new();
	// add \033 is because the problem left over by history
	// for the get_table_index is start with index 1
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id = 1;"));

	gint db_index, table_index;
	guint sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);

	// hit dbgrop index 0 of dbgroups
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 1;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id < 10;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id < 100 and id > 20;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);
	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id in(11,21);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id in(11,23);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);
	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id > 0;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
    ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);
	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

exit:
    g_array_free(hit_db_groups, TRUE);
    g_array_free(shard_group, TRUE);
    clear_mock_sharding_table_rule(&sharding_table_rule);
}

void test_sharding_get_dbgroups_by_range() {
	GArray* hit_db_groups = g_array_new(FALSE, TRUE, sizeof(guint));
	sharding_table_t sharding_table_rule;

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(&sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);


	/**
	 *  select or delete
	 */
	GPtrArray *sql_tokens = sql_tokens_new();
	// add \033 is because the problem left over by history
	// for the get_table_index is start with index 1
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id = 1;"));

	gint db_index, table_index;
	guint sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	sharding_result_t ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);

	// hit dbgrop index 0 of dbgroups
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id = 99;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id = 200;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id = 10000;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_OUT_OF_RANGE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE nonshard_key = 1;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1, 150);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1, 50) AND id = 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1, 50) OR id = 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(2, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1, 50) AND id > 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(3, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1, 50) OR id > 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(3, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1, 1);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id IN(1) OR id > 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(3, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
	sql_tokens_free(sql_tokens);

    // between
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 1 AND 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 200 AND 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_WRONG_RANGE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 200 AND 200;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 200 AND 299;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 100 AND 200;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 1000 AND 2000;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_OUT_OF_RANGE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 200 AND 1000;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 55 AND 255;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 0 AND 99 OR id = 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 0 AND 99 AND id = 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id BETWEEN 1 AND 99 OR id >= 100"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
    sql_tokens_free(sql_tokens);

    // range
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 50 AND id < 250"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
    sql_tokens_free(sql_tokens);

    // auto makeup range, 50 - 100 and 200 - 250
	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 50 AND id < 250 AND id < 100 AND id > 200"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    /** -- TODO --
     * mix "and or" distribute to all shard now
     */
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 50 AND id < 100 OR id > 200"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ALL_SHARD);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 50 AND id < 100 AND id > 200"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id >= 50 AND id <= 100"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id < 50 AND id < 200"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id < 50"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 99"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(2, ==, hit_db_groups->len);
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 1));
    sql_tokens_free(sql_tokens);
    
    /** -- TODO -- */
    /*actually mysql will return empty set, but for simple, it will send to three shard*/
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 150 AND id = 50"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test_shard WHERE id > 150 OR id = 50"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(3, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    g_assert_cmpint(1, ==, g_array_index(hit_db_groups, guint, 1));
    g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 2));
    sql_tokens_free(sql_tokens);
    /**
     * delete 
     */
    
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id = 99"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);
    
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id <= 99"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id > 99"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id BETWEEN 0 AND 150"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id >= 0 AND id <= 150"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id = 1 OR id = 150"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
    /*below is a empty query, but it will proxy to two backend shard*/
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id = 1 AND id = 150"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id = 1 OR id > 150"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    g_assert_cmpint(0, ==, hit_db_groups->len);
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id BETWEEN 0 AND 99"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id > 50 AND id < 100 OR id > 200"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
    sql_tokens_free(sql_tokens);
    
    sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033DELETE FROM test_shard WHERE id >= 50 AND id <= 60"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

    g_assert_cmpint(ret, ==, SHARDING_RET_OK);
    g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
    sql_tokens_free(sql_tokens);

    /**
	 * insert or replace
	 */
	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033INSERT INTO test_shard(id, other) VALUES(1, 0);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033REPLACE INTO test_shard(id, other) VALUES(1, 0);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033INSERT INTO test_shard VALUES(200, 0);")); // default is the first value as the shard key
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(2, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033INSERT INTO test_shard(id, other) VALUES(1, 0), (150, 1);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033INSERT INTO test_shard(id, other) VALUES(1, 0), (2, 1);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	/**
	 * update
	 */
	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id = 1;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
	g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id IN (1, 200);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id IN (1, 2);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id IN (1);"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id IN (1) AND id = 50;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id IN (1) OR id = 50;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id IN (1) OR id = 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id < 99;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id > 0 and id < 99;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id BETWEEN 0 AND 99;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id BETWEEN 0 AND 50 OR id = 99;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_OK);
	g_assert_cmpint(1, ==, hit_db_groups->len);
    g_assert_cmpint(0, ==, g_array_index(hit_db_groups, guint, 0));
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id BETWEEN 0 AND 50 OR id = 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id BETWEEN 0 AND 150;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id < 199;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

    /* below sql only update id = 50, but it seems like a error sql, for simple, atlas will return error*/
	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id = 50 and id > 199;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033UPDATE test_shard SET other = 0 WHERE id = 50 OR id > 199;"));
	sql_type = get_table_index(sql_tokens, &db_index, &table_index);
	g_array_set_size(hit_db_groups, 0);
	ret = sharding_get_dbgroups(hit_db_groups, &sharding_table_rule, sql_tokens, table_index, sql_type);

	g_assert_cmpint(ret, ==, SHARDING_RET_ERR_MULTI_SHARD_WRITE);
	g_assert_cmpint(0, ==, hit_db_groups->len);
	sql_tokens_free(sql_tokens);

exit:
	g_array_free(hit_db_groups, TRUE);
	g_array_free(shard_group, TRUE);
	clear_mock_sharding_table_rule(&sharding_table_rule);
}

void test_sharding_lookup_table_shard_rule() {
	sharding_table_t *sharding_table_rule = g_new0(sharding_table_t, 1);

	// init mock shard group
	GArray *shard_group = g_array_new(FALSE, FALSE, sizeof(group_range_map_t));
	add_mock_group_range_map(shard_group, 0, 99, 0);
	add_mock_group_range_map(shard_group, 100, 199, 1);
	add_mock_group_range_map(shard_group, 200, 299, 2);

	init_mock_sharding_table_rule(sharding_table_rule, "test_shard", SHARDING_TYPE_RANGE, "id", shard_group);
    GHashTable *sharding_table_rules = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, sharding_table_free);
    g_hash_table_insert(sharding_table_rules, "test.test_shard", sharding_table_rule);

    sql_tokens_param_t sql_tokens_param;
    GPtrArray* sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * from test_shard WHERE id = 0;"));
    sql_tokens_param.sql_tokens = sql_tokens;
	sql_tokens_param.sql_type = get_table_index(sql_tokens, &sql_tokens_param.db_index, &sql_tokens_param.table_index);

    sharding_table_t* ret = sharding_lookup_table_shard_rule(sharding_table_rules, "test", &sql_tokens_param);
    g_assert_cmpint(ret, ==, sharding_table_rule);
    g_hash_table_remove_all(sharding_table_rules);
    g_hash_table_destroy(sharding_table_rules);
	g_array_free(shard_group, TRUE);
}

void test_sharding_get_ro_backend() {
    network_backends_t* backends = network_backends_new(1, NULL);
    char address[128] = "192.168.0.1@100";
    network_backends_add(backends, address, BACKEND_TYPE_RO);
    strcpy(address, "192.168.0.2@50");
    network_backends_add(backends, address, BACKEND_TYPE_RO);
    strcpy(address, "192.168.0.3@1");
    network_backends_add(backends, address, BACKEND_TYPE_RO);
    strcpy(address, "192.168.0.4");
    network_backends_add(backends, address, BACKEND_TYPE_RW);

    guint i;
    for (i = 0; i < network_backends_count(backends); ++i) {
        network_backend_t *backend = network_backends_get(backends, i);
        backend->state = BACKEND_STATE_UP;
    }

    GHashTable* counter = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, g_free);

    guint test_count = 151;
    for (i = 0; i < test_count; ++i) {
        network_backend_t *chose_backend = sharding_get_ro_backend(backends);
        g_assert_cmpint(NULL, !=, chose_backend);
        guint* chose_count = g_hash_table_lookup(counter, chose_backend);
        if (chose_count == NULL) {
            chose_count = g_new0(guint, 1);
            *chose_count = 1;
            g_hash_table_insert(counter, chose_backend, chose_count);
        } else {
            (*chose_count)++;
        }
    }

    for (i = 0; i < network_backends_count(backends); ++i) {
        network_backend_t *backend = network_backends_get(backends, i);
        if (backend->type != BACKEND_TYPE_RO) { continue; }

        guint* chose_count = g_hash_table_lookup(counter, backend);
        g_assert(chose_count != NULL);
        g_assert_cmpint(*chose_count, ==, backend->weight);
    }

    g_hash_table_remove_all(counter);
    g_hash_table_destroy(counter);
    network_backends_free(backends);
}

void test_sharding_is_support_sql() {
	GPtrArray* sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT GET_LOCK('20090529', 60);"));
    gboolean is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test LIMIT 100 OFFSET 10;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test LIMIT 100, 10;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT COUNT(*) FROM test;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT MAX(id) FROM test;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT id AS alias_name FROM test;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT SQL_CALC_FOUND_ROWS * FROM test;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test ORDER BY id;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT * FROM test GROUP BY id;"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);

	sql_tokens = sql_tokens_new();
	sql_tokenizer(sql_tokens, C("\033SELECT College.cName, College.state, College.enrollment, \
                Apply.cName, Apply.major, Apply.decision                                      \
                FROM  College LEFT OUTER JOIN                                                 \
                        Apply ON College.cName = Apply.cName"));
    is_support = sharding_is_support_sql(sql_tokens);
    g_assert_cmpint(FALSE, ==, is_support);
	sql_tokens_free(sql_tokens);
}

long getTime() {  
    struct timeval iTime;  
    gettimeofday(&iTime, NULL);  
    long lTime = ((long) iTime.tv_sec) * 1000000 + (long) iTime.tv_usec;  
    return lTime;  
}  

void test_sql_tokenizer_performance() {
    int i;
    long time_start = getTime();
    for (i = 0; i < 1000000; i++) {
        GPtrArray* sql_tokens = sql_tokens_new();
        sql_tokenizer(sql_tokens, C("\033SELECT id, name FROM test WHERE name = 'test' AND id = 1000;"));

        const sql_token** tokens = (const sql_token**)(sql_tokens->pdata);
        guint token_len = sql_tokens->len;
        
        if (tokens[1]->token_id == TK_SQL_SELECT) {
            int j;
            for (j = 2; j < token_len; j++) {
                /* find FROM*/
                if (tokens[j]->token_id == TK_SQL_FROM) { break; }
            }

            if ( j+1 < token_len && strcasecmp("test", tokens[j+1]->text->str) == 0) {
                /*FIND WHERE*/
                for (;j < token_len; j++) {
                    if (tokens[j]->token_id == TK_SQL_WHERE) { break; }
                }
                /* find id */
                for (j=j+1; j < token_len; j++) {
                    if (tokens[j]->token_id == TK_LITERAL && strcasecmp("id", tokens[j]->text->str) == 0 && j+1 < token_len && tokens[j+1]->token_id == TK_EQ) {break;}
                }
            }
        }
        sql_tokens_free(sql_tokens);
    }   

    long time_end = getTime();

    printf("time escaped:%ld\n", time_end - time_start);
}

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	/* g_test_add_func("/core/test_sharding_get_dbgroups_by_range", test_sharding_get_dbgroups_by_range); */
	/* g_test_add_func("/core/test_sharding_get_dbgroups_by_hash", test_sharding_get_dbgroups_by_hash); */

    /* g_test_add_func("/core/test_sharding_lookup_table_shard_rule", test_sharding_lookup_table_shard_rule); */
    /* g_test_add_func("/core/test_sharding_get_ro_backend", test_sharding_get_ro_backend); */
    g_test_add_func("/core/test_sql_tokenizer_performance", test_sql_tokenizer_performance);
    //g_test_add_func("/core/test_sharding_is_support_sql", test_sharding_is_support_sql);
	return g_test_run();
}
