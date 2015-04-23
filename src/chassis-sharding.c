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

#include <strings.h>
#include <time.h>
#include <stdio.h>

#include "glib-ext.h"
#include "chassis-sharding.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "network-socket.h"
//#include "lib/sql-tokenizer.h"


#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define IS_SHARDKEY_RANGE_TYPE(type)         \
    (type == SHARDING_SHARDKEY_VALUE_GT  ||  \
     type == SHARDING_SHARDKEY_VALUE_GTE ||  \
     type == SHARDING_SHARDKEY_VALUE_LT  ||  \
     type == SHARDING_SHARDKEY_VALUE_LTE)

#define IS_SHARDKEY_GT_OR_GE(type)              \
        (type == SHARDING_SHARDKEY_VALUE_GT ||  \
        type == SHARDING_SHARDKEY_VALUE_GTE)

#define IS_SHARDKEY_LT_OR_LE(type)              \
        (type == SHARDING_SHARDKEY_VALUE_LT ||  \
         type == SHARDING_SHARDKEY_VALUE_LTE)

G_INLINE_FUNC shard_key_type_t sql_token_id2shard_key_type(int token_id) {
    switch (token_id) {
        case TK_GT:
            return SHARDING_SHARDKEY_VALUE_GT;
        case TK_GE:
            return SHARDING_SHARDKEY_VALUE_GTE;
        case TK_LT:
            return SHARDING_SHARDKEY_VALUE_LT;
        case TK_LE:
            return SHARDING_SHARDKEY_VALUE_LTE;
        case TK_NE:
            return SHARDING_SHARDKEY_VALUE_NE;
        case TK_EQ:
            return SHARDING_SHARDKEY_VALUE_EQ;
        defalut:
            return SHARDING_SHARDKEY_VALUE_UNKNOWN;  // never come here
    }

    return SHARDING_SHARDKEY_VALUE_UNKNOWN;  // never come here
}

G_INLINE_FUNC void init_value_shard_key_t(shard_key_t *shard_key_obj, shard_key_type_t type, gint64 value) {
    shard_key_obj->type = type;
    shard_key_obj->value = value;
    shard_key_obj->range_begin = shard_key_obj->range_end = -1;
}

G_INLINE_FUNC void init_range_shard_key_t(shard_key_t *shard_key_obj, gint64 range_begin, gint64 range_end) 
{
    shard_key_obj->type = SHARDING_SHARDKEY_RANGE;
    shard_key_obj->value = -1;
    shard_key_obj->range_begin = range_begin;
    shard_key_obj->range_end = range_end;
}

G_INLINE_FUNC shard_key_type_t reverse_shard_key(shard_key_type_t type) {
    switch(type) {
        case SHARDING_SHARDKEY_VALUE_GT:
            return SHARDING_SHARDKEY_VALUE_LTE;
        case SHARDING_SHARDKEY_VALUE_GTE:
            return SHARDING_SHARDKEY_VALUE_LT;
        case SHARDING_SHARDKEY_VALUE_LT:
            return SHARDING_SHARDKEY_VALUE_GTE;
        case SHARDING_SHARDKEY_VALUE_LTE:
            return SHARDING_SHARDKEY_VALUE_GT;
        case SHARDING_SHARDKEY_VALUE_EQ:
            return SHARDING_SHARDKEY_VALUE_NE;
        case SHARDING_SHARDKEY_VALUE_NE:
            return SHARDING_SHARDKEY_VALUE_EQ;
        defalut:
            return SHARDING_SHARDKEY_VALUE_UNKNOWN;
    }
}

static gint gint64_compare_func(gconstpointer value1, gconstpointer value2) {
    if (*((gint64*)value1) > *((gint64*)value2)) {
        return 1;
    } else if (*((gint64*)value1) == *((gint64*)value2)) {
        return 0;
    } else {
        return -1;
    }
}

G_INLINE_FUNC sharding_result_t value_list_shard_key_append(GArray *shard_keys, Expr *expr, const char *shardkey_name, gboolean is_not_opr) {
    char value_buf[64] = {0};

    if (expr->pLeft != NULL && LEMON_TOKEN_STRING(expr->pLeft->op) && expr->pList != NULL && 
            strncasecmp(shardkey_name, (const char*) expr->pLeft->token.z, expr->pLeft->token.n) == 0) 
    {
        gint i;
        if (is_not_opr) {
            GArray *value_list = g_array_sized_new(FALSE, FALSE, sizeof(gint64), expr->pList->nExpr);
            for (i = 0; i < expr->pList->nExpr; i++) {
                Expr *value_expr = expr->pList->a[i].pExpr;
                if (value_expr->op != TK_INTEGER) { continue; }

                dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
                gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
                g_array_unique_append_val(value_list, &shard_key_value, gint64_compare_func); // value in value_list is sorted by g_array_unique_append_val
            }
            if (value_list->len > 0) {
                shard_key_t shardkey_obj;
                gint64 shardkey_value1 = g_array_index(value_list, gint64, 0);
                init_value_shard_key_t(&shardkey_obj, SHARDING_SHARDKEY_VALUE_LT, shardkey_value1);
                g_array_append_val(shard_keys, shardkey_obj);
                
                for (i = 1; i < value_list->len; i++) {
                    gint64 shardkey_value2 = g_array_index(value_list, gint64, i);
                    gint64 range_begin = shardkey_value1+1, range_end = shardkey_value2-1;
                    if (range_begin <= range_end) {
                        init_range_shard_key_t(&shardkey_obj, range_begin, range_end);
                        g_array_append_val(shard_keys, shardkey_obj);
                    }
                    shardkey_value1 = shardkey_value2;
                }

                shardkey_value1 = g_array_index(value_list, gint64, value_list->len-1);
                init_value_shard_key_t(&shardkey_obj, SHARDING_SHARDKEY_VALUE_GT, shardkey_value1);
                g_array_append_val(shard_keys, shardkey_obj);
            }
            g_array_free(value_list, TRUE);
        } else {
            for (i = 0; i < expr->pList->nExpr; i++) {
                Expr *value_expr = expr->pList->a[i].pExpr;
                if (value_expr->op != TK_INTEGER) { continue; }

                dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
                gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
                shard_key_t shardkey_obj;
                init_value_shard_key_t(&shardkey_obj, SHARDING_SHARDKEY_VALUE_EQ, shard_key_value);
                g_array_append_val(shard_keys, shardkey_obj);
            }
        }

        return SHARDING_RET_OK;
    } else {
        return SHARDING_RET_ERR_NO_SHARDKEY;
    }
}

G_INLINE_FUNC sharding_result_t value_shard_key_append(GArray *shard_keys, Expr *expr, const char *shardkey_name, gboolean is_not_opr) {
    char value_buf[64] = {0};
    shard_key_t shardkey1, shardkey2;

    if (expr->pLeft != NULL && LEMON_TOKEN_STRING(expr->pLeft->op) &&expr->pRight != NULL && expr->pRight->op == TK_INTEGER &&
            strncasecmp(shardkey_name, (const char*)expr->pLeft->token.z, expr->pLeft->token.n) == 0) 
    {
        dup_token2buff(value_buf, sizeof(value_buf), expr->pRight->token);
        gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
        shard_key_type_t type = sql_token_id2shard_key_type(expr->op);
        if (is_not_opr) {
            type = reverse_shard_key(type);
        }
        if (type == SHARDING_SHARDKEY_VALUE_NE) {
            init_value_shard_key_t(&shardkey1, SHARDING_SHARDKEY_VALUE_GT, shard_key_value);
            init_value_shard_key_t(&shardkey2, SHARDING_SHARDKEY_VALUE_LT, shard_key_value);
            g_array_append_val(shard_keys, shardkey1);
            g_array_append_val(shard_keys, shardkey2);
        } else {
            init_value_shard_key_t(&shardkey1, type, shard_key_value);
            g_array_append_val(shard_keys, shardkey1);
        }
        return SHARDING_RET_OK;
    } else {
        return SHARDING_RET_ERR_NO_SHARDKEY;
    }
}

G_INLINE_FUNC sharding_result_t between_shard_key_append(GArray *shard_keys, Expr *expr, const char *shardkey_name, gboolean is_not_opr) {
    char value_buf[64] = {0};
    shard_key_t shardkey1, shardkey2;

    if (expr->pLeft != NULL && LEMON_TOKEN_STRING(expr->pLeft->op) && expr->pList != NULL && expr->pList->nExpr == 2 &&
            strncasecmp(shardkey_name, (const char*)expr->pLeft->token.z, expr->pLeft->token.n) == 0) 
    {
        Expr *begin_expr = expr->pList->a[0].pExpr, *end_expr = expr->pList->a[1].pExpr;
        if (begin_expr->op != TK_INTEGER || end_expr->op != TK_INTEGER) {
            return SHARDING_RET_OK;
        }

        dup_token2buff(value_buf, sizeof(value_buf), begin_expr->token);
        gint range_begin = g_ascii_strtoll(value_buf, NULL, 10);
        dup_token2buff(value_buf, sizeof(value_buf), end_expr->token);
        gint range_end = g_ascii_strtoll(value_buf, NULL, 10);
        if (is_not_opr) {
            init_value_shard_key_t(&shardkey1, SHARDING_SHARDKEY_VALUE_LT, range_begin);
            init_value_shard_key_t(&shardkey2, SHARDING_SHARDKEY_VALUE_GT, range_end);
            g_array_append_val(shard_keys, shardkey1);
            g_array_append_val(shard_keys, shardkey2);
        } else {
            if (range_begin > range_end) {
                return SHARDING_RET_OK;
            }
            init_range_shard_key_t(&shardkey1, range_begin, range_end);
            g_array_append_val(shard_keys, shardkey1);
        }

        return SHARDING_RET_OK;
    } else {
        return SHARDING_RET_ERR_NO_SHARDKEY;
    }
}


G_INLINE_FUNC void merge_eq_and_eq(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value == shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_eq_and_ne(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value != shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_eq_and_gt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value > shardkey2->value) { // id = 10 AND id > 10
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_eq_and_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) { // id = 5 AND id >= 10
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_eq_and_lt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value < shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_eq_and_lte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_eq_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->range_begin && shardkey1->value <= shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey1);
    }
}

G_INLINE_FUNC void merge_ne_and_ne(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1, shardkey_merge2, shardkey_merge3;
    if (shardkey1->value == shardkey2->value) { // id != 10 AND id != 10 ---> id > 10 OR id < 10
        init_value_shard_key_t(&shardkey_merge1, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value);
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    } else if (shardkey1->value > shardkey2->value) { // id != 10 AND id != 5 ---> id > 10 OR (id > 5 AND id < 10) OR id < 5
        init_value_shard_key_t(&shardkey_merge1, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value);
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey2->value);
        init_range_shard_key_t(&shardkey_merge3, shardkey2->value+1, shardkey1->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
        g_array_append_val(shard_keys, shardkey_merge3);
    } else {// id != 5 AND id != 10 ---> id > 10 OR (id > 5 AND id < 10) OR id < 5
        init_value_shard_key_t(&shardkey_merge1, SHARDING_SHARDKEY_VALUE_GT, shardkey2->value);
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value);
        init_range_shard_key_t(&shardkey_merge3, shardkey1->value+1, shardkey2->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
        g_array_append_val(shard_keys, shardkey_merge3);
    }
}

G_INLINE_FUNC void merge_ne_and_gt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value <= shardkey2->value) { // id != 10 AND id > 10 --> id > 10
        g_array_append_val(shard_keys, *shardkey2);
    } else { // "id != 10 AND id > 5" ---> "id > 5 AND id < 10 OR id > 10"
        init_range_shard_key_t(&shardkey_merge1, shardkey2->value+1, shardkey1->value-1); 
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }
}

G_INLINE_FUNC void merge_ne_and_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value < shardkey2->value) { // id != 10 AND id >= 50 ---> id >= 50
        g_array_append_val(shard_keys, *shardkey2);
    } else if (shardkey1->value == shardkey2->value) { // id != 10 AND id >= 100 ---> id > 100
        shardkey2->type = SHARDING_SHARDKEY_VALUE_GT;
        g_array_append_val(shard_keys, *shardkey2);
    } else { // "id != 10 AND id >= 5" ---> "id >= 5 AND id < 10 OR id > 10"
        init_range_shard_key_t(&shardkey_merge1, shardkey2->value, shardkey1->value-1); 
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }
}

G_INLINE_FUNC void merge_ne_and_lt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value >= shardkey2->value) { // id != 5 and id < 5 ---> id < 5
        g_array_append_val(shard_keys, *shardkey2);
    } else { // id != 10 and id < 20 --> "id > 10 AND id < 20 OR id < 10"
        init_range_shard_key_t(&shardkey_merge1, shardkey1->value+1, shardkey2->value-1); 
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }
}

G_INLINE_FUNC void merge_ne_and_lte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value > shardkey2->value) { // id != 6 and id <= 5 ---> id <= 5
        g_array_append_val(shard_keys, *shardkey2);
    } else if (shardkey1->value == shardkey2->value) {
        shardkey2->type = SHARDING_SHARDKEY_VALUE_LT;
        g_array_append_val(shard_keys, *shardkey2);
    } else { // id != 10 and id <= 20 --> "id > 10 AND id <= 20 OR id < 10"
        init_range_shard_key_t(&shardkey_merge1, shardkey1->value+1, shardkey2->value); 
        init_value_shard_key_t(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }
}

G_INLINE_FUNC void merge_ne_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value < shardkey2->range_begin || shardkey1->value > shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);
    } else { // id != 10 AND id >= 5 AND id <= 20 ---> id >= 5 AND id < 10 OR id > 10 AND id <= 20
        init_range_shard_key_t(&shardkey_merge1, shardkey2->range_begin, shardkey1->value-1);
        init_range_shard_key_t(&shardkey_merge2, shardkey1->value+1, shardkey2->range_end);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }    
}

G_INLINE_FUNC void merge_lt_and_gt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1;

    if (shardkey1->value > shardkey2->value) { 
        // id < 10 AND id > 5 ---> id > 5 AND id < 10
        init_range_shard_key_t(&shardkey_merge1, shardkey2->value+1, shardkey1->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);
    } // id < 5 AND id > 5 ---> empty.
}

G_INLINE_FUNC void merge_lt_and_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1;

    if (shardkey1->value > shardkey2->value) { 
        // id < 10 AND id >= 5 ---> id >= 5 AND id < 10
        init_range_shard_key_t(&shardkey_merge1, shardkey2->value, shardkey1->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);
    }// id < 5 AND id >= 6 ---> empty.
}

G_INLINE_FUNC void merge_lt_and_lt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) { // id < 5 AND id < 5
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id < 10 AND id < 5 --> id < 5
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_lt_and_lte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) { // id < 5 AND id <= 5
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id < 6 AND id <= 5
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_lt_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->range_begin) {
        return;
    } else if (shardkey1->value > shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);
    } else { // id < 10 AND id >= 5 AND id <= 20 ---> id >= 5 AND id < 10 
        shardkey2->range_end = shardkey1->value - 1;
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_lte_and_gt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1;

    if (shardkey2->value < shardkey1->value) {
        // id <= 10 and id > 5 ---> id > 5 and id <= 10
        init_range_shard_key_t(&shardkey_merge1, shardkey2->value+1, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
    }
}

G_INLINE_FUNC void merge_lte_and_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    shard_key_t shardkey_merge1;

    if (shardkey2->value > shardkey1->value) {
        return;
    } else if (shardkey1->value == shardkey2->value) {
        shardkey1->type = SHARDING_SHARDKEY_VALUE_EQ;
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id <= 10 and id >= 5 ---> id >= 5 and id <= 10
        init_range_shard_key_t(&shardkey_merge1, shardkey2->value, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
    }
}

G_INLINE_FUNC void merge_lte_and_lte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
     if (shardkey1->value <= shardkey2->value) { // id <= 5 AND id <= 5
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id <= 10 AND id <= 5 --> id <= 5
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_lte_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value < shardkey2->range_begin) {
        return;
    } else if (shardkey1->value >= shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);
    } else { // id < 10 AND id >= 5 AND id <= 20 ---> id >= 5 AND id < 10 
        shardkey2->range_end = shardkey1->value;
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_gt_and_gt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    } else {
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_gt_and_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    } else {
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_gt_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->range_end) {
        return;
    } else if (shardkey1->value < shardkey2->range_begin) {
        g_array_append_val(shard_keys, *shardkey2);
    } else {
        shardkey2->range_begin = shardkey1->value + 1;
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_gte_and_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);
    } else {
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_gte_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->value > shardkey2->range_end) {
        return;
    } else if (shardkey1->value <= shardkey2->range_begin) {
        g_array_append_val(shard_keys, *shardkey2);
    } else {
        shardkey2->range_begin = shardkey1->value;
        g_array_append_val(shard_keys, *shardkey2);
    }
}

G_INLINE_FUNC void merge_range_and_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    if (shardkey1->range_begin > shardkey2->range_end || shardkey2->range_begin > shardkey1->range_end) {
        return;
    } else if (shardkey1->range_begin <= shardkey2->range_end) {
         shardkey1->range_end = shardkey2->range_end;
        g_array_append_val(shard_keys, *shardkey1);
    } else if (shardkey2->range_begin <= shardkey1->range_end) {
         shardkey1->range_begin = shardkey2->range_begin;
        g_array_append_val(shard_keys, *shardkey1);
    } else if (shardkey1->range_begin <= shardkey2->range_begin && shardkey1->range_end >= shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);
    } else if (shardkey2->range_begin <= shardkey1->range_begin && shardkey2->range_end >= shardkey1->range_end) {
        g_array_append_val(shard_keys, *shardkey1);
    }
}

/**
 * shardkey1->type is SHARDING_SHARDKEY_VALUE_EQ
 */
void and_opr_merge_shardkey1_value_eq(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_eq(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_eq_and_ne(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_eq_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_eq_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_eq_and_lt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_eq_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_eq_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 
} 

void and_opr_merge_shardkey1_value_ne(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_ne(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_ne(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GT: 
            merge_ne_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_ne_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_ne_and_lt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_ne_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_ne_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 
}

void and_opr_merge_shardkey1_value_lt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_lt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_lt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT: 
            merge_lt_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_lt_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_lt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lt_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_lt_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 
}

void and_opr_merge_shardkey1_value_lte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_lte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_lte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT: 
            merge_lte_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_lte_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_lte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_lte_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 

}

void and_opr_merge_shardkey1_value_gt(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT: 
            merge_gt_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_gt_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_gt_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 
}

void and_opr_merge_shardkey1_value_gte(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT: 
            merge_gt_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_gte_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_gte_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 
}

void and_opr_merge_shardkey1_value_range(GArray *shard_keys, shard_key_t *shardkey1, shard_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_gt_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_gte_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_range_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    } 
}

void merge_shard_key_by_and_opr(GArray *shard_keys, GArray *left_shardkeys, GArray *right_shardkeys) {
    guint i, j;
    // empty AND xxx ---> empty
    // xxx  AND empty ---> empty
    for (i = 0; i < left_shardkeys->len; i++) { 
        shard_key_t *shardkey1 = &g_array_index(left_shardkeys, shard_key_t, i);

        for (j = 0; j < right_shardkeys->len; j++) {
            shard_key_t *shardkey2 = &g_array_index(right_shardkeys, shard_key_t, j);

            switch(shardkey1->type) {
                case SHARDING_SHARDKEY_VALUE_EQ:
                    and_opr_merge_shardkey1_value_eq(shard_keys, shardkey1, shardkey2);
                    break; 
                case SHARDING_SHARDKEY_VALUE_NE:
                    and_opr_merge_shardkey1_value_ne(shard_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_LT:
                    and_opr_merge_shardkey1_value_lt(shard_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_LTE:
                    and_opr_merge_shardkey1_value_lte(shard_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_GT:
                    and_opr_merge_shardkey1_value_gt(shard_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_GTE:
                    and_opr_merge_shardkey1_value_gte(shard_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_RANGE:
                    and_opr_merge_shardkey1_value_range(shard_keys, shardkey1, shardkey2);
                    break;
                default:
                    break;
            }
        }
    }
}

/**
 * postorder traversal
 */
sharding_result_t find_shard_key_inwhere(GArray *shard_keys, Expr *expr, const char *shardkey_name, gboolean is_not_opr) {
    GArray *left_shardkeys = NULL, *right_shardkeys = NULL;
    sharding_result_t ret_left, ret_right, ret = SHARDING_RET_OK;
    
    int logic_opr = expr->op;
    switch(logic_opr) {
        case TK_GE:
        case TK_GT:
        case TK_LE:
        case TK_LT:
        case TK_EQ:
        case TK_NE:
            ret = value_shard_key_append(shard_keys, expr, shardkey_name, is_not_opr);
            break;
        case TK_IN:
            ret = value_list_shard_key_append(shard_keys, expr, shardkey_name, is_not_opr);
            break;
        case TK_BETWEEN:
            ret = between_shard_key_append(shard_keys, expr, shardkey_name, is_not_opr); 
            break;
        case TK_NOT:
            return find_shard_key_inwhere(shard_keys, expr->pLeft, shardkey_name, !is_not_opr);
        case TK_AND:
        case TK_OR:
            goto postorder_traversal;
        defalut: 
            break;
    }

exit:
    return ret;

postorder_traversal:
    // -- TODO -- optimize tips, left_shardkeys or right_shardkeys not use GArray, implement it myself.
    // which use the stack firstly, if need to append to more than one shardkey, then realloc it.
    left_shardkeys = g_array_sized_new(FALSE, FALSE, sizeof(shard_key_t), 2); 
    right_shardkeys = g_array_sized_new(FALSE, FALSE, sizeof(shard_key_t), 2);
    ret_left = find_shard_key_inwhere(left_shardkeys, expr->pLeft, shardkey_name, is_not_opr);
    ret_right = find_shard_key_inwhere(right_shardkeys, expr->pRight, shardkey_name, is_not_opr);
    if (logic_opr == TK_AND && ret_left == SHARDING_RET_OK && ret_right == SHARDING_RET_OK) {
        merge_shard_key_by_and_opr(shard_keys, left_shardkeys, right_shardkeys);
    } else { 
        /* no need to merge "TK_OR" shardkeys, or may left child have no shardkey or 
         * right child have no shardkey
         */
        g_array_append_vals(shard_keys, left_shardkeys->data, left_shardkeys->len);
        g_array_append_vals(shard_keys, right_shardkeys->data, right_shardkeys->len);
    }
    if (left_shardkeys) { g_array_free(left_shardkeys, TRUE); }
    if (right_shardkeys) { g_array_free(right_shardkeys, TRUE); }
    
    return (ret_left == SHARDING_RET_ERR_NO_SHARDKEY && 
            ret_right == SHARDING_RET_ERR_NO_SHARDKEY) ? ret_left : SHARDING_RET_OK;
}

static sharding_result_t parse_sharding_keys_from_where_expr(GArray *shard_keys, const sharding_table_t *sharding_table_rule, parse_info_t *parse_info) {
    Parse *parse_obj = parse_info->parse_obj;
    Expr *where_expr = parse_get_where_expr(parse_obj);

    if (where_expr == NULL) {
        return SHARDING_RET_ALL_SHARD;
    }
    
    return find_shard_key_inwhere(shard_keys, where_expr, sharding_table_rule->shard_key->str, FALSE);
}

static sharding_result_t parse_sharding_keys_from_insert_sql(GArray* shard_keys, const sharding_table_t *sharding_table_rule, parse_info_t *parse_info) {
    Parse *parse_obj = parse_info->parse_obj;

    Insert *insert_obj = parse_obj->parsed.array[0].result.insertObj;
    int i;
    char value_buf[64] = {0};
    shard_key_t shard_key_obj;

    if (insert_obj->pSetList != NULL) { // INSERT INTO test(name) SET name = 'test'; 
        for (i = 0; i < insert_obj->pSetList->nExpr; i++) {
            Expr *value_expr = insert_obj->pSetList->a[i].pExpr;
            if (value_expr->op == TK_INTEGER && strcasecmp(sharding_table_rule->shard_key->str, insert_obj->pSetList->a[i].zName) == 0) {
                dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
                gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
                init_value_shard_key_t(&shard_key_obj, SHARDING_SHARDKEY_VALUE_EQ, shard_key_value);
                g_array_append_val(shard_keys, shard_key_obj);
                break;
            }
        } 

        if (i == insert_obj->pSetList->nExpr) {
            return SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN;
        }
    } else if (insert_obj->pValuesList != NULL) { 
        if (insert_obj->pColumn != NULL) {
            for (i = 0; i < insert_obj->pColumn->nId; i++) {
                if (strcasecmp(sharding_table_rule->shard_key->str, insert_obj->pColumn->a[i].zName) == 0) {
                    break;
                }
            }

            if (i == insert_obj->pColumn->nId) {
                return SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN;
            }
        } else {
            i = 0;  // default is 0
        }
        
        gint value_index = i;
        for (i = 0; i < insert_obj->pValuesList->nValues; i++) {
            ExprList *value_list = insert_obj->pValuesList->a[i];
            Expr *value_expr = value_list->a[value_index].pExpr;
            if (value_expr->op == TK_INTEGER) {
                dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
                gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
                init_value_shard_key_t(&shard_key_obj, SHARDING_SHARDKEY_VALUE_EQ, shard_key_value);
                g_array_append_val(shard_keys, shard_key_obj);
            }
        }
    }

    return SHARDING_RET_OK;
}

static sharding_result_t sharding_get_shard_keys(GArray *shard_keys, const sharding_table_t *sharding_table_rule, parse_info_t *parse_info) {
    int sqltype = parse_info->parse_obj->parsed.array[0].sqltype;
    if(sqltype == SQLTYPE_SELECT || sqltype == SQLTYPE_DELETE || sqltype == SQLTYPE_UPDATE) { 
        return parse_sharding_keys_from_where_expr(shard_keys, sharding_table_rule, parse_info);
    } else if (sqltype == SQLTYPE_INSERT || sqltype == SQLTYPE_REPLACE) { 
        return parse_sharding_keys_from_insert_sql(shard_keys, sharding_table_rule, parse_info);
    }


    return SHARDING_RET_ERR_NOT_SUPPORT;
}

static gint guint_compare_func(gconstpointer value1, gconstpointer value2) {
    if (*((guint*)value1) > *((guint*)value2)) {
        return 1;
    } else if (*((guint*)value1) == *((guint*)value2)) {
        return 0;
    } else {
        return -1;
    }
}

static void sharding_get_dbgroup_by_range(GArray* hited_db_groups, shard_key_t *shard_key, const sharding_table_t *sharding_table_rule) {
    guint i = 0;
    GArray *shard_groups = sharding_table_rule->shard_group;

    for (i = 0; i < shard_groups->len; ++i) {
        group_range_map_t *range_map = &g_array_index(shard_groups, group_range_map_t, i);
        if( shard_key->type == SHARDING_SHARDKEY_VALUE_EQ ) {
            if (range_map->range_begin <= shard_key->value && shard_key->value <= range_map->range_end) {
                g_array_unique_append_val(hited_db_groups, &range_map->db_group_index, guint_compare_func);
                break;
            }
        } else if (shard_key->type == SHARDING_SHARDKEY_RANGE) { // if shardkey range begin or end are contained in range_map range, will hit the db group
            if ((range_map->range_begin <= shard_key->range_begin && shard_key->range_begin <= range_map->range_end) ||
                (range_map->range_begin <= shard_key->range_end && shard_key->range_end <= range_map->range_end) ||
                (shard_key->range_begin < range_map->range_begin && range_map->range_end < shard_key->range_end)) // shardkey's range is include range_map's range
            {
                g_array_unique_append_val(hited_db_groups, &range_map->db_group_index, guint_compare_func);
                // -- TODO --
                // below code is optimization tips:
                // it needs the elems of sharding_table_rule->shard_group to be ordered by range
                /* if (shard_key->range_end <= range_map->range_end) { */
                /*     break; */
                /* } */
                continue;
            }
        } else if (shard_key->type == SHARDING_SHARDKEY_VALUE_GT) {
            if (shard_key->value < range_map->range_end) {
                g_array_unique_append_val(hited_db_groups, &range_map->db_group_index, guint_compare_func);
                continue;
            }
        } else if (shard_key->type == SHARDING_SHARDKEY_VALUE_GTE) {
            if (shard_key->value <= range_map->range_end) {
                g_array_unique_append_val(hited_db_groups, &range_map->db_group_index, guint_compare_func);
                continue;
            }
        } else if (shard_key->type == SHARDING_SHARDKEY_VALUE_LT) {
            if (range_map->range_begin < shard_key->value ) {
                g_array_unique_append_val(hited_db_groups, &range_map->db_group_index, guint_compare_func);
                continue;
            }
        } else if (shard_key->type == SHARDING_SHARDKEY_VALUE_LTE) {
            if (range_map->range_begin <= shard_key->value) {
                g_array_unique_append_val(hited_db_groups, &range_map->db_group_index, guint_compare_func);
                continue;
            }
        }
    }
}


static sharding_result_t sharding_get_dbgroup_by_hash(GArray* hited_db_groups, shard_key_t *shard_key, const sharding_table_t *sharding_table_rule) {
    guint i = 0;
    GArray *shard_groups = sharding_table_rule->shard_group;
    gint group_count = shard_groups->len;
    if (shard_key->type == SHARDING_SHARDKEY_VALUE_EQ) {
        gint group_index = shard_key->value % group_count;
        for (i = 0; i < group_count; i++) {
            group_hash_map_t hm = g_array_index(shard_groups, group_hash_map_t, i);
            if (hm.db_group_index == group_index) break;
        }
        if (i < group_count) {
            g_array_unique_append_val(hited_db_groups, &group_index, guint_compare_func);
        }
    }else { // -- TODO -- replace with return SHARDING_RET_ALL_SHARD, because if the shard_key->len > 1, the branch will run more than once
        /* for (i = 0; i < group_count; i++) { */
        /*     group_hash_map_t hm = g_array_index(shard_groups, group_hash_map_t, i); */
        /*     g_array_append_val(hited_db_groups, hm.db_group_index); */
        /* } */
        g_array_set_size(hited_db_groups, 0);
        return SHARDING_RET_ALL_SHARD;
    }

    return SHARDING_RET_OK;
}

static gboolean sharding_is_write_sql(Parse *parse_obj) {
    if (parse_obj == NULL) { return TRUE; }
    ParsedResultItem *parsed_result = &parse_obj->parsed.array[0];
    
    int sqltype = parsed_result->sqltype;
    switch (sqltype) {
        case SQLTYPE_SELECT:
        case SQLTYPE_SET_NAMES:
        case SQLTYPE_SET_CHARACTER_SET:
        case SQLTYPE_SET:
            return FALSE;
        default:
            return TRUE;
    }

    return TRUE;
}

sharding_result_t sharding_get_dbgroup_by_range_wrapper(GArray* hit_db_groups, const sharding_table_t *sharding_table_rule, GArray* shard_keys, gboolean is_write_sql) {
    sharding_result_t ret = SHARDING_RET_OK;

    guint i = 0;
    for (i = 0; i < shard_keys->len; ++i) {
        shard_key_t *shard_key = &g_array_index(shard_keys, shard_key_t, i);
        sharding_get_dbgroup_by_range(hit_db_groups, shard_key, sharding_table_rule);
    }

    if (hit_db_groups->len > 1 && is_write_sql) { // hit more than one dbgroup
        g_array_set_size(hit_db_groups, 0);
        ret = SHARDING_RET_ERR_MULTI_SHARD_WRITE; 
    } else if (hit_db_groups->len == 0) {
         ret = SHARDING_RET_ERR_WRONG_RANGE;
    }
exit:
    return ret;
}

sharding_result_t sharding_get_dbgroup_by_hash_wrapper(GArray* hit_db_groups, const sharding_table_t *sharding_table_rule, GArray *shard_keys, gboolean is_write_sql) {
    sharding_result_t ret = SHARDING_RET_OK;

    guint i = 0;
    for (i = 0; i < shard_keys->len; ++i) {
        shard_key_t *shard_key = &g_array_index(shard_keys, shard_key_t, i);
        ret = sharding_get_dbgroup_by_hash(hit_db_groups, shard_key, sharding_table_rule);
        if (ret == SHARDING_RET_ALL_SHARD && is_write_sql && sharding_table_rule->shard_group->len > 1) {
            ret = SHARDING_RET_ERR_MULTI_SHARD_WRITE;
            goto exit;
        }
        if (ret != SHARDING_RET_OK) { return ret; }
    }

    if (hit_db_groups->len > 1 && is_write_sql) { // hit more than one dbgroup
        g_array_set_size(hit_db_groups, 0);
        ret = SHARDING_RET_ERR_MULTI_SHARD_WRITE;
    } else if (hit_db_groups->len == 0) {
        ret = SHARDING_RET_ERR_WRONG_HASH; 
    }

exit:
    return ret;
}


sharding_result_t sharding_get_dbgroups(GArray* hit_db_groups, const sharding_table_t* sharding_table_rule, parse_info_t *parse_info) {
    GArray *shard_keys = g_array_sized_new(FALSE, TRUE, sizeof(shard_key_t), 4);

    sharding_result_t ret = SHARDING_RET_OK;
    ret = sharding_get_shard_keys(shard_keys, sharding_table_rule, parse_info);

    parse_info->is_write_sql = sharding_is_write_sql(parse_info->parse_obj);
    if (ret == SHARDING_RET_ALL_SHARD && parse_info->is_write_sql && sharding_table_rule->shard_group->len > 1) {
        ret = SHARDING_RET_ERR_MULTI_SHARD_WRITE;
        goto exit;
    }

    if (ret != SHARDING_RET_OK) { goto exit; }
    if (shard_keys->len == 0 ) {
        ret = SHARDING_RET_ERR_HIT_NOTHING;
        goto exit;
    }

    if (sharding_table_rule->shard_type == SHARDING_TYPE_RANGE) {
        ret = sharding_get_dbgroup_by_range_wrapper(hit_db_groups, sharding_table_rule, shard_keys, parse_info->is_write_sql);
    } else if (sharding_table_rule->shard_type == SHARDING_TYPE_HASH) {
        ret = sharding_get_dbgroup_by_hash_wrapper(hit_db_groups, sharding_table_rule, shard_keys, parse_info->is_write_sql);
    }
    
exit:
    g_array_free(shard_keys, TRUE);
    return ret;
}

gint verify_group_index(GKeyFile* keyfile, sharding_table_t *rule) {
    gchar **groups, **gname;
    int i, j, length, gindex, ret = 0;
    if(keyfile == NULL || rule == NULL) return 0;
    GArray *group_index_array = g_array_new(FALSE, TRUE, sizeof(int));
    groups = g_key_file_get_groups(keyfile, &length);
    for(i = 0; i < length; i++) {
        gname = g_strsplit(groups[i], "-", 2);
        if(gname == NULL || gname[0] == NULL || gname[1] == NULL) {
            ret = -1;
            goto exit;
        }
        if(strcasecmp(gname[0], "group") == 0) {
            gindex = atoi(gname[1]);
            g_array_append_val(group_index_array, gindex);
        }
        g_strfreev(gname);
    }
    if(rule->shard_type == SHARDING_TYPE_HASH) {
        for(i = 0; i < rule->shard_group->len; i++) {
            group_hash_map_t hm = g_array_index(rule->shard_group, group_hash_map_t, i);
            for(j = 0; j < group_index_array->len; j++) {
                gindex = g_array_index(group_index_array, int, j);
                if(hm.db_group_index == gindex) break;
            }
            if(j == group_index_array->len) {
                ret = -1;
                goto exit;
            }
        }
    }else {
        for(i = 0; i < rule->shard_group->len; i++) {
            group_range_map_t gm = g_array_index(rule->shard_group, group_range_map_t, i);
            for(j = 0; j < group_index_array->len; j++) {
                gindex = g_array_index(group_index_array, int, j);
                if(gm.db_group_index == gindex) break;
            }
            if(j == group_index_array->len) {
                ret = -1;
                goto exit;
            }
        }
    }
exit:
    g_array_free(group_index_array, TRUE);
    g_strfreev(groups);
    return ret;
}

/*read the GKeyFile, and set sharding_table*/
sharding_table_t* keyfile_to_sharding_table(GKeyFile* keyfile, gchar* group_name) {
    int i = 0;
    gchar **groups = NULL;
    gchar **range_map = NULL;
    gchar *type = NULL, *table_name = NULL, *shard_key = NULL;
    sharding_table_t *rule = NULL;

    GOptionEntry config_entries[] = {
        {"table", 0, 0, G_OPTION_ARG_STRING, NULL, "the name of sharding table", NULL},
        {"type", 0, 0, G_OPTION_ARG_STRING, NULL, "shard type", NULL},
        {"shard-key", 0, 0, G_OPTION_ARG_STRING, NULL, "shard key", NULL},
        {"groups", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "the groups for this table sharding", NULL},
        {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
    };
    config_entries[i++].arg_data = &(table_name);
    config_entries[i++].arg_data = &(type);
    config_entries[i++].arg_data = &(shard_key);
    config_entries[i++].arg_data = &(groups);
    if(-1 == chassis_keyfile_to_options(keyfile, group_name, config_entries)) {
        g_message("%s:chassis_keyfile_to_options error", G_STRLOC);
        return NULL;
    }
    if(strchr(table_name, '.') == NULL) {
        g_message("%s:%s must include the database name.", G_STRLOC, table_name);
        g_free(table_name);
        g_free(type);
        g_strfreev(groups);
        g_free(shard_key);
        return NULL;
    }
    rule = g_new0(sharding_table_t, 1);
    rule->table_name = g_string_new(table_name);
    rule->shard_key = g_string_new(shard_key);
    if(strcasecmp(type, "hash") == 0) {
        group_hash_map_t hm;
        rule->shard_type = SHARDING_TYPE_HASH;
        rule->shard_group = g_array_new(FALSE, TRUE, sizeof(group_hash_map_t));
        for(i = 0; groups && groups[i]; i++) {
            hm.db_group_index = atoi(groups[i]);
            g_array_append_val(rule->shard_group, hm);
        }
        if(verify_group_index(keyfile, rule) != 0)
            g_error("%s:the group indexs in shardrule are not mapping.", G_STRLOC);
    }else {
        group_range_map_t rm;
        rule->shard_type = SHARDING_TYPE_RANGE;
        rule->shard_group = g_array_new(FALSE, TRUE, sizeof(group_range_map_t));
        for(i = 0; groups && groups[i]; i++) {
            gchar **gvec = g_strsplit(groups[i], ":", 2);
            if(gvec[0]) {
                rm.db_group_index = atoi(gvec[0]);
                if(gvec[1]) {
                    gchar **range = g_strsplit(gvec[1], "-", 2);
                    if(range[0] && range[1]) {
                        rm.range_begin = g_ascii_strtoll(range[0], NULL, 10);

                        if(strcmp(range[1], "*") == 0) {
                            rm.range_end = G_MAXINT64;
                        } else {
                            rm.range_end = g_ascii_strtoll(range[1], NULL, 10);
                        }
                        
                        if (rm.range_begin > rm.range_end) {
                            g_error("group:%d range(%d-%d) begin must less than end!", rm.db_group_index, rm.range_begin, rm.range_end);
                        }
                    }else {
                        g_error("group:%d the range rule is wrong", rm.db_group_index);
                    }
                    g_strfreev(range);
                }
            }
            g_strfreev(gvec);
            g_array_append_val(rule->shard_group, rm);
        }
        if(verify_group_index(keyfile, rule) != 0)
            g_error("%s:the group indexs in shardrule are not mapping.", G_STRLOC);
    }

    g_free(table_name);
    g_free(type);
    g_free(shard_key);
    g_strfreev(groups);
    return rule;

}

db_group_t* keyfile_to_db_group(GKeyFile* keyfile, gchar* group_name, guint event_thread_count) {
    int i = 0;
    gchar **gvec = NULL;
    gchar **backend_addresses = NULL;
    gchar **read_only_backend_addresses = NULL;

    GOptionEntry config_entries[] = {
        { "proxy-read-only-backend-addresses", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "address:port of the remote slave-server", "<host:port>" },
        { "proxy-backend-addresses", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "address:port of the remote backend-servers", "<host:port>" },
        {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
    };

    config_entries[i++].arg_data = &(read_only_backend_addresses);
    config_entries[i++].arg_data = &(backend_addresses);
    if(-1 == chassis_keyfile_to_options(keyfile, group_name, config_entries)) {
        g_message("%s:chassis_keyfile_to_options error", G_STRLOC);
        return NULL;
    }
    gvec = g_strsplit(group_name, "-", 2);
    if(gvec == NULL || gvec[1] == NULL) {
        g_message("%s:the format of group name(%s) error.", G_STRLOC, group_name);
        g_strfreev(gvec);
        return NULL;
    }
    db_group_t *dg = g_new0(db_group_t, 1);
    dg->group_id = atoi(gvec[1]);
    dg->bs = network_backends_new(event_thread_count, NULL);
    for (i = 0; backend_addresses && backend_addresses[i]; i++) {
        if (-1 == network_backends_add(dg->bs, backend_addresses[i], BACKEND_TYPE_RW)) {
            g_strfreev(gvec);
            g_strfreev(backend_addresses);
            g_strfreev(read_only_backend_addresses);
            return NULL;
        }
    }

    for (i = 0; read_only_backend_addresses && read_only_backend_addresses[i]; i++) {
        if (-1 == network_backends_add(dg->bs, read_only_backend_addresses[i], BACKEND_TYPE_RO)) {
            g_strfreev(gvec);
            g_strfreev(backend_addresses);
            g_strfreev(read_only_backend_addresses);
            return NULL;
        }
    }

    g_strfreev(gvec);
    g_strfreev(backend_addresses);
    g_strfreev(read_only_backend_addresses);
    return dg;
}

void sharding_table_free(sharding_table_t *table) {
    g_string_free(table->table_name, TRUE);
    g_string_free(table->shard_key, TRUE);
    g_array_free(table->shard_group, TRUE);
    g_free(table);
}

static gboolean is_contain_notsupport_tokens(Select *select_obj) {
    if (select_obj->pLimit && select_obj->pOffset) { // LIMIT OFFSET
        return TRUE;
    }

    if (select_obj->op == TK_UNION || select_obj->pRightmost != NULL || select_obj->pPrior != NULL) { // UNION
        return TRUE;
    }

    if (select_obj->pGroupBy || select_obj->pHaving || select_obj->pOrderBy ) { // group by, having
        return TRUE;
    } 
   
    if (select_obj->pSrc) {
        guint i;
        for (i = 0; i < select_obj->pSrc->nSrc; i++) {
            if (select_obj->pSrc->a[0].jointype != 0) { // JOIN
                return TRUE;
            }
        }
    }

    return FALSE;
}

/**
 * -- TODO --
 * SUM(), AVG(), and so on.
 */ 
static gboolean is_notsupport_sql_function(Token *func_token) {
    return TRUE;
}

/**
 * figure out if the sql is supported
 */ 
gboolean sharding_is_support_sql(Parse *parse_obj) {
    if (parse_obj == NULL) { return FALSE; }
    if (parse_obj->parsed.curSize > 1) { return FALSE; }

    SqlType sqltype = parse_obj->parsed.array[0].sqltype;
    
    if (SQLTYPE_SELECT == sqltype) {
        Select *select_obj = parse_obj->parsed.array[0].result.selectObj;
        if (select_obj->pEList) {
            guint i;
            for (i = 0; i < select_obj->pEList->nExpr; i++) {
                Expr *expr = select_obj->pEList->a[i].pExpr;
                if (expr->op == TK_FUNCTION && strncasecmp("GET_LOCK", (const char*)expr->token.z, expr->token.n) == 0) {
                    return FALSE;
                }
            }      
        }
    } 

    switch(sqltype) {
        case SQLTYPE_SELECT:
        case SQLTYPE_INSERT:
        case SQLTYPE_REPLACE:
        case SQLTYPE_DELETE:
        case SQLTYPE_UPDATE:
            return TRUE;
        defalut:
            return FALSE;
    }
    return FALSE;

}


sharding_table_t* sharding_lookup_table_shard_rule(GHashTable* table_shard_rules, gchar* default_db, parse_info_t *parse_info)
{
    if (parse_info->table_name == NULL) return NULL;
    
    gchar* full_table_name = NULL;
    if (parse_info->db_name == NULL) {
        full_table_name = g_strdup_printf("%s.%s", default_db, parse_info->table_name);
    } else {
        full_table_name = g_strdup_printf("%s.%s", parse_info->db_name, parse_info->table_name);
    }
    sharding_table_t *sharding_table_rule = (sharding_table_t*)g_hash_table_lookup(table_shard_rules, full_table_name);

    if (full_table_name) { g_free(full_table_name); }

    return sharding_table_rule;
}

gboolean sharding_is_contain_multishard_notsupport_feature(Parse *parse_obj) {
    ParsedResultItem *parsed_result = &parse_obj->parsed.array[0];
    if (parsed_result->sqltype == SQLTYPE_SELECT) {
        Select *select_obj = parsed_result->result.selectObj;
        ExprList *column_list = select_obj->pEList;
        int i;
        for (i = 0; i < column_list->nExpr; i++) {
            Expr *column_expr = column_list->a[i].pExpr;
            if (column_expr->op == TK_FUNCTION && is_notsupport_sql_function(&column_expr->token)) {
                return TRUE;
            } else if (is_contain_notsupport_tokens(select_obj)) {
                return TRUE;
            } 
        }
    }

    return FALSE;
}

void sharding_set_connection_flags(GPtrArray* sql_tokens, network_mysqld_con* con) {
    /** -- TODO --
     * complete this function
     */

    //con->is_in_select_calc_found_rows = FALSE;
    /* if (token_len > 2) { */
    /* 	if (tokens[1]->token_id == TK_SQL_SELECT && strcasecmp(tokens[2]->text->str, "GET_LOCK") == 0) { */
    /* 		gchar* key = tokens[4]->text->str; */
    /* 		if (!g_hash_table_lookup(con->locks, key)) g_hash_table_add(con->locks, g_strdup(key)); */
    /* 	} */

    /* 	if (token_len > 4) {	//SET AUTOCOMMIT = {0 | 1} */
    /* 		if (tokens[1]->token_id == TK_SQL_SET && tokens[3]->token_id == TK_EQ) { */
    /* 			if (strcasecmp(tokens[2]->text->str, "AUTOCOMMIT") == 0) { */
    /* 				char* str = tokens[4]->text->str; */
    /* 				if (strcmp(str, "0") == 0) con->is_not_autocommit = TRUE; */
    /* 				else if (strcmp(str, "1") == 0) con->is_not_autocommit = FALSE; */
    /* 			} */
    /* 		} */
    /* 	} */
    /* } */

    /* guint i; */
    /* for (i = 1; i < len; ++i) { */
    /* sql_token* token = ts[i]; */
    /* if (ts[i]->token_id == TK_SQL_SQL_CALC_FOUND_ROWS) { */
    /* con->is_in_select_calc_found_rows = TRUE; */
    /* break; */
    /* } */
    /* } */
}

sharding_dbgroup_context_t* sharding_lookup_dbgroup_context(GHashTable* dbgroup_contexts, guint groupid) {
    g_assert(dbgroup_contexts != NULL);
    sharding_dbgroup_context_t* dbgroup_context = (sharding_dbgroup_context_t*)g_hash_table_lookup(dbgroup_contexts, &groupid);
    if (dbgroup_context == NULL) {
        guint *insert_groupid = g_new(guint, 1);
        *insert_groupid = groupid;
        dbgroup_context = sharding_dbgroup_context_new(groupid);
        g_hash_table_insert(dbgroup_contexts, insert_groupid, dbgroup_context);
    }

    return dbgroup_context;
}

sharding_dbgroup_context_t* sharding_dbgroup_context_new(guint group_id) {
    sharding_dbgroup_context_t *context = g_new0(sharding_dbgroup_context_t, 1);
    context->group_id = group_id;
    context->st = network_mysqld_con_lua_new();

    context->charset_client = g_string_new(NULL);
    context->charset_results = g_string_new(NULL);
    context->charset_connection = g_string_new(NULL);

    context->parse.command = -1;
    return context;
}

void sharding_dbgroup_context_free(gpointer data) {
    sharding_dbgroup_context_t* context = (sharding_dbgroup_context_t*) data;
 
    if (context->st) network_mysqld_con_lua_free(context->st);
    if (context->backend_sock) {
        network_socket_free(context->backend_sock);
        context->backend_sock = NULL;
    }

	if (context->parse.data && context->parse.data_free) {
		context->parse.data_free(context->parse.data);
        context->parse.data = NULL;
        context->parse.data_free = NULL;
	}
    g_free(context);
    g_string_free(context->charset_client, TRUE);
    g_string_free(context->charset_results, TRUE);
    g_string_free(context->charset_connection, TRUE);
}

static void sharding_merge_rows_elem_free(gpointer data) {
    if (data) { g_ptr_array_free((GPtrArray*)data, TRUE); }
}

void sharding_reset_trans_context_t(trans_context_t* trans_ctx) {
    if (trans_ctx == NULL) { return; }

    trans_ctx->is_default_dbgroup_in_trans = FALSE;
    trans_ctx->trans_stage = TRANS_STAGE_INIT;
    trans_ctx->in_trans_dbgroup_ctx = NULL;
    if (trans_ctx->origin_begin_sql) {
        g_string_free(trans_ctx->origin_begin_sql, TRUE);
        trans_ctx->origin_begin_sql = NULL;   
    }
}

sharding_context_t* sharding_context_new() {
    sharding_context_t *sharding_context = g_new0(sharding_context_t, 1);
    sharding_context->merge_result.recvd_rows = g_ptr_array_new_with_free_func(sharding_merge_rows_elem_free); // elem is GPtrArray
    sharding_context->querying_dbgroups = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);
    return sharding_context;
}

void sharding_context_free(gpointer data) {
    sharding_context_t* context = (sharding_context_t*) data;
    if (context->merge_result.recvd_rows){
        g_ptr_array_set_size(context->merge_result.recvd_rows, 0); // sharding_merge_rows_elem_free will be called automatelly
        g_ptr_array_free(context->merge_result.recvd_rows, TRUE);
    }

    if (context->querying_dbgroups) {
        g_hash_table_remove_all(context->querying_dbgroups);
        g_hash_table_destroy(context->querying_dbgroups);
    }

    g_free(data);
}

void sharding_querying_dbgroup_context_add(GHashTable* querying_dbgroups, sharding_dbgroup_context_t* dbgroup_ctx) {
    if (dbgroup_ctx == NULL || dbgroup_ctx->backend_sock == NULL) { return; }
    int* socket_fd = g_new(int, 1);
    *socket_fd = dbgroup_ctx->backend_sock->fd;
    g_hash_table_insert(querying_dbgroups, socket_fd, dbgroup_ctx);    
}


void sharding_context_reset(sharding_context_t* context) {
    if (context == NULL) { return; }

    context->query_sent_count = 0;
    context->is_continue_from_send_query = FALSE;
    g_hash_table_remove_all(context->querying_dbgroups);
    sharding_reset_merge_result(context);
}

void sharding_reset_merge_result(sharding_context_t* context) {
    if (context->merge_result.recvd_rows) {
        //GPtrArray* recvd_rows = context->merge_result.recvd_rows;
        //guint i, len = recvd_rows->len;
        /* for (i = 0; i < len; ++i) { // -- BUG-FIX -- free make core dump */
        /*     sharding_merge_rows_elem_free(g_ptr_array_index(recvd_rows, i)); */
        /* } */
        g_ptr_array_set_size(context->merge_result.recvd_rows, 0); // sharding_merge_rows_elem_free will be called automatelly
    }

    context->merge_result.result_recvd_num = 0;
    context->merge_result.warnings = 0;
    context->merge_result.affected_rows = 0;
    context->merge_result.shard_num = 0;
    context->merge_result.limit = G_MAXINT;
}

void sharding_reset_st(network_mysqld_con_lua_t* st) {
    if (st == NULL) { return; }

    st->injected.sent_resultset = 0;
    network_injection_queue_reset(st->injected.queries);
}



network_backend_t* sharding_get_rw_backend(network_backends_t *backends) {
    guint count = network_backends_count(backends);
    network_backend_t* backend = NULL;
    guint i;

    for (i = 0; i < count; ++i) {
        backend = network_backends_get(backends, i);
        if (backend == NULL) { continue; }

        if (chassis_event_thread_pool(backend) == NULL) { continue; }
    
        if (backend->type == BACKEND_TYPE_RW && backend->state == BACKEND_STATE_UP) {
            break;
        }
    }

    return backend;
}

/**
 * load balance by weight of slaves
 */
network_backend_t* sharding_get_ro_backend(network_backends_t* backends) {
    g_wrr_poll *rwsplit = backends->global_wrr;
    
    guint i;
    if (rwsplit->max_weight == 0) {
        guint count = network_backends_count(backends); 
        for (i = 0; i < count; ++i) { // -- TODO -- lock backends->backends_mutex here 
            network_backend_t* backend = network_backends_get(backends, i);
            if (backend == NULL) continue;
            if (rwsplit->max_weight < backend->weight) {
                rwsplit->max_weight = backend->weight;
                rwsplit->cur_weight = backend->weight;
            }
        }
    }

    guint max_weight = rwsplit->max_weight;
    guint cur_weight = rwsplit->cur_weight;
    guint next_ndx = rwsplit->next_ndx;

    network_backend_t* found_backend = NULL;
    guint backend_count = network_backends_count(backends);
    for (i = 0; i < backend_count; ++i) {
        network_backend_t* backend = network_backends_get(backends, next_ndx);
        if (backend == NULL) { goto next; }
        if(chassis_event_thread_pool(backend) == NULL) {  goto next; }

        if (backend->type == BACKEND_TYPE_RO && backend->weight >= cur_weight && backend->state == BACKEND_STATE_UP) {
            found_backend = backend;
        }

    next:
        if (next_ndx >= backend_count - 1) {
            --cur_weight;
            next_ndx = 0;

            if (cur_weight == 0) { cur_weight = max_weight; }
        } else {
            ++next_ndx;
        }

        if (found_backend != NULL) { break; }
    }
    rwsplit->cur_weight = cur_weight;
    rwsplit->next_ndx = next_ndx;
    return found_backend;
}

network_backend_t* sharding_read_write_split(Parse *parse_obj, network_backends_t* backends) {
    network_backend_t* backend = NULL;

    TokenArray *tokens_array = &parse_obj->tokens;
    if (tokens_array->curSize > 0) {
        TokenItem *token_item = &tokens_array->array[0];
        if (token_item->tokenType == TK_COMMENT && strncasecmp("/*MASTER*/", (const char*)token_item->token.z, token_item->token.n) == 0) {
            return sharding_get_rw_backend(backends);
        }
    } 

    backend = sharding_get_ro_backend(backends);
    
    if (backend == NULL) { return sharding_get_rw_backend(backends); }
    return backend;
}

void sharding_proxy_error_send_result(network_mysqld_con* con) {
    gboolean is_first_packet = TRUE;
    network_socket* recv_sock = con->client;
    network_socket* send_sock = con->client;
    GString* packet = NULL;

    // release recvd packets
    while((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) {
        if (is_first_packet) {
            network_packet p;
            p.data = packet;
            p.offset = 0;

            network_mysqld_con_reset_command_response_state(con);
            // track the command-states
            // for commnd e.g. "LOAD DATA LOCAL INFILE"
            if (0 != network_mysqld_con_command_states_init(con, &p)) {
				g_debug("%s", G_STRLOC);
            }
            is_first_packet = FALSE;
        }

        g_string_free(packet, TRUE);
    }

    GList* cur;
    /* if we don't send the query to the backend, it won't be tracked. So track it here instead
     * to get the packet tracking right (LOAD DATA LOCAL INFILE, ...) */
    for (cur = send_sock->send_queue->chunks->head; cur; cur = cur->next) {
        network_packet p;
        int r;

        p.data = cur->data;
        p.offset = 0;

        r = network_mysqld_proto_get_query_result(&p, con);
    }

    con->state = CON_STATE_SEND_QUERY_RESULT;
    con->resultset_is_finished = TRUE; /* we don't have more too send */
}

// PROXY_SEND_INJECTION
void sharding_proxy_send_injections(sharding_dbgroup_context_t* dbgroup_context) {
    injection *inj = network_injection_queue_peek_head(dbgroup_context->st->injected.queries);
    dbgroup_context->resultset_is_needed = inj->resultset_is_needed;

    network_socket* send_sock = dbgroup_context->backend_sock;
    network_mysqld_queue_reset(send_sock);
    network_mysqld_queue_append(send_sock, send_sock->send_queue, S(inj->query));
}

void sharding_proxy_send_result(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context, injection* inj) {
    network_socket* recv_sock = dbgroup_context->backend_sock;
    network_socket* send_sock = con->client;
    network_mysqld_con_lua_t* st = dbgroup_context->st;

    gboolean have_last_insert_id = inj->qstat.insert_id > 0; // this is a write query
    ++st->injected.sent_resultset;
    GString* packet = NULL;
    if(st->injected.sent_resultset == 1) { 
        while((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) { network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, packet); }
    } else { // when sql has syntax error, the resultset may be sent before by other shard response
        if (con->resultset_is_needed) {
            while ((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) { g_string_free(packet, TRUE); }
        }
    }
    
    // return back to connection pool
    if (!have_last_insert_id && con->trans_context->trans_stage != TRANS_STAGE_IN_TRANS) {
        network_connection_pool_sharding_add_connection(con, dbgroup_context);       
    }
    network_mysqld_queue_reset(send_sock);
}

void sharding_proxy_ignore_result(sharding_dbgroup_context_t* dbgroup_context) {
    network_socket* recv_sock = dbgroup_context->backend_sock;
    if (dbgroup_context->resultset_is_needed) {
        GString* packet = NULL;
        while((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) { g_string_free(packet, TRUE); }
    }
}
    
void sharding_modify_db(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context) {
    if (dbgroup_context->backend_sock == NULL || dbgroup_context->backend_sock->default_db == NULL) { return; }

    char* default_db = con->client->default_db->str;

    if (default_db != NULL && strcmp(default_db, dbgroup_context->backend_sock->default_db->str) != 0) {
        gchar cmd = COM_INIT_DB;
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, default_db);
        injection* inj = injection_new(INJECTION_INIT_DB, query);
        inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_context->st->injected.queries, inj);
    }
}


void sharding_modify_charset(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context) {
    g_string_truncate(dbgroup_context->charset_client, 0);
    g_string_truncate(dbgroup_context->charset_results, 0);
    g_string_truncate(dbgroup_context->charset_connection, 0);

    if (dbgroup_context->backend_sock == NULL) return;

    gboolean is_set_client      = FALSE;
    gboolean is_set_results     = FALSE;
    gboolean is_set_connection  = FALSE;
    
    // check if the charset is the same between client socket and backend socket
    network_socket* client = con->client;
    network_socket* server = dbgroup_context->backend_sock;

    gchar cmd = COM_QUERY;

	if (!is_set_client && !g_string_equal(client->charset_client, server->charset_client)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_CLIENT=");
		g_string_append(query, client->charset_client->str);
		g_string_assign(dbgroup_context->charset_client, client->charset_client->str);

		injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_CLIENT_SQL, query);
		inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_context->st->injected.queries, inj);
	}

	if (!is_set_results && !g_string_equal(client->charset_results, server->charset_results)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_RESULTS=");
		g_string_append(query, client->charset_results->str);
		g_string_assign(dbgroup_context->charset_results, client->charset_results->str);

		injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_RESULTS_SQL, query);
		inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_context->st->injected.queries, inj);
	}

	if (!is_set_connection && !g_string_equal(client->charset_connection, server->charset_connection)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_CONNECTION=");
		g_string_append(query, client->charset_connection->str);
		g_string_assign(dbgroup_context->charset_connection, client->charset_connection->str);

		injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_CONNECTION_SQL, query);
		inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_context->st->injected.queries, inj);
	}
}

void sharding_modify_user(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context, GHashTable *pwd_table) {
    if (dbgroup_context->backend_sock == NULL) { return; }
    GString* client_user = con->client->response->username;
    GString* server_user = dbgroup_context->backend_sock->response->username;

    if (!g_string_equal(client_user, server_user)) {
        GString* com_change_user = g_string_new(NULL);

		g_string_append_c(com_change_user, COM_CHANGE_USER);
		g_string_append_len(com_change_user, client_user->str, client_user->len + 1);

		GString* hashed_password = g_hash_table_lookup(pwd_table, client_user->str);
		if (!hashed_password) return;

		GString* expected_response = g_string_sized_new(20);
		network_mysqld_proto_password_scramble(expected_response, S(dbgroup_context->backend_sock->challenge->challenge), S(hashed_password));

		g_string_append_c(com_change_user, (expected_response->len & 0xff));
		g_string_append_len(com_change_user, S(expected_response));
		g_string_append_c(com_change_user, 0);

		injection* inj = injection_new(INJECTION_MODIFY_USER_SQL, com_change_user);
		inj->resultset_is_needed = TRUE;
		network_injection_queue_prepend(dbgroup_context->st->injected.queries, inj);

		g_string_truncate(con->client->response->response, 0);
		g_string_assign(con->client->response->response, expected_response->str);
		g_string_free(expected_response, TRUE);
    }
}

void sharding_inject_trans_begin_sql(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_ctx) {
    trans_context_t* trans_ctx = con->trans_context;
    if (trans_ctx->trans_stage != TRANS_STAGE_BEFORE_SEND_BEGIN) { return; }
    
    g_assert(trans_ctx->is_default_dbgroup_in_trans == FALSE && trans_ctx->origin_begin_sql != NULL);
    
    injection* inj = injection_new(INJECTION_TRANS_BEGIN_SQL, trans_ctx->origin_begin_sql);
    inj->resultset_is_needed = TRUE;
    trans_ctx->origin_begin_sql = NULL;
    
    trans_ctx->trans_stage = TRANS_STAGE_SENDING_BEGIN;
    network_mysqld_con_lua_t* st = (network_mysqld_con_lua_t*)dbgroup_ctx->st;
    network_injection_queue_prepend(st->injected.queries, inj);
}

const gchar* sharding_get_error_msg(sharding_result_t ret) {
    switch(ret) {
        case SHARDING_RET_OK:
            return "Proxy Warning - no error!";
        case SHARDING_RET_ALL_SHARD:
            return "Proxy Warning - no error!";
        case SHARDING_RET_ERR_HIT_NOTHING:
            return "Proxy Warning - where clause don't hit any shard!";
        case SHARDING_RET_ERR_NO_SHARDKEY:
            return "Proxy Warning - no shard key";
        case SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN:
            return "Proxy Warning - please check your sql syntax, no shard column is given!";
        case SHARDING_RET_ERR_MULTI_SHARD_WRITE:
            return "Proxy Warning - write operation is only allow to one dbgroup!";
        case SHARDING_RET_ERR_OUT_OF_RANGE:
            return "Proxy Warning - your sql query is out of range, please check your config file or your sql!";
        case SHARDING_RET_ERR_WRONG_RANGE:
            return "Proxy Warning - your sql query range is wrong!";
        case SHARDING_RET_ERR_NOT_SUPPORT:
            return "Proxy Warning - sorry, the sql is not supported yet!";
        case SHARDING_RET_ERR_UNKNOWN:
            return "Proxy Warning - unknown sharding error!";
        case SHARDING_RET_ERR_WRONG_HASH:
            return "Proxy Warning - SHARDING_RET_ERR_WRONG_HASH";
    }

    return "";
}


/**
 * reset the command-response parsing
 *
 * some commands needs state information and we have to
 * reset the parsing as soon as we add a new command to the send-queue
 */
// correspond to network_mysqld_con_reset_command_response_state 
void sharding_dbgroup_reset_command_response_state(struct network_mysqld_con_parse* parse) {
	parse->command = -1;
	if (parse->data && parse->data_free) {
		parse->data_free(parse->data);

		parse->data = NULL;
		parse->data_free = NULL;
	}
}

/**
 * correspond to network_mysqld_con_command_states_init
 */
gint sharding_parse_command_states_init(struct network_mysqld_con_parse* parse, network_mysqld_con* con, network_packet* packet, 
        sharding_dbgroup_context_t* dbgroup_ctx) 
{
    guint8 cmd;
    int err = 0;

    err = err || network_mysqld_proto_skip_network_header(packet);
    err = err || network_mysqld_proto_get_int8(packet, &cmd);

    if (err) return -1;

    parse->command = cmd;
    packet->offset = 0; /* reset the offset again for the next function*/

    switch(parse->command) {
        case COM_QUERY:
        case COM_PROCESS_INFO:
        case COM_STMT_EXECUTE:
            parse->data = network_mysqld_com_query_result_new();
            parse->data_free = (GDestroyNotify) network_mysqld_com_query_result_free;
            break;
        case COM_STMT_PREPARE:
            parse->data = network_mysqld_com_stmt_prepare_result_new();
            parse->data_free = (GDestroyNotify)network_mysqld_com_stmt_prepare_result_free;
            break;
        case COM_INIT_DB:
            parse->data = network_mysqld_com_init_db_result_new();
            parse->data_free = (GDestroyNotify)network_mysqld_com_init_db_result_free;

            network_mysqld_com_init_db_result_track_state(packet, parse->data);

            break;
        case COM_QUIT:
            /* track COM_QUIT going to the server, to be able to tell if the server
             * a) simply went away or
             * b) closed the connection because the client asked it to
             * If b) we should not print a message at the next EV_READ event from the server fd
             */
            dbgroup_ctx->com_quit_seen = TRUE;
        default:
            break;
	}

	return 0;
}

/**
 * @param packet the current packet that is passing by
 *
 * @return -1 on invalid packet, 
 *          0 need more packets, 
 *          1 for the last packet 
 */
// correspond to network_mysqld_proto_get_query_result
gint sharding_parse_get_query_result(struct network_mysqld_con_parse* parse, network_mysqld_con* con, network_packet* packet) {
  	guint8 status;
	int is_finished = 0;
	int err = 0;
	network_mysqld_eof_packet_t *eof_packet;
	
	err = err || network_mysqld_proto_skip_network_header(packet);
	if (err) return -1;

	/* forward the response to the client */
	switch (parse->command) {
	case COM_CHANGE_USER: 
		/**
		 * - OK
		 * - ERR (in 5.1.12+ + a duplicate ERR)
		 */
		err = err || network_mysqld_proto_get_int8(packet, &status);
		if (err) return -1;

		switch (status) {
		case MYSQLD_PACKET_ERR:
			is_finished = 1;
			break;
		case MYSQLD_PACKET_OK:
			is_finished = 1;
			break;
		default:
			g_error("%s.%d: COM_(0x%02x) should be (ERR|OK), got %02x",
					__FILE__, __LINE__,
					parse->command, status);
			break;
		}
		break;
	case COM_INIT_DB: 
        is_finished = sharding_mysqld_proto_get_com_init_db(packet, parse->data, con);
		break;
	case COM_REFRESH:
	case COM_STMT_RESET:
	case COM_PING:
	case COM_TIME:
	case COM_REGISTER_SLAVE:
	case COM_PROCESS_KILL:
		err = err || network_mysqld_proto_get_int8(packet, &status);
		if (err) return -1;

		switch (status) {
		case MYSQLD_PACKET_ERR:
		case MYSQLD_PACKET_OK:
			is_finished = 1;
			break;
		default:
			g_error("%s.%d: COM_(0x%02x) should be (ERR|OK), got 0x%02x",
					__FILE__, __LINE__,
					parse->command, (guint8)status);
			break;
		}
		break;
	case COM_DEBUG:
	case COM_SET_OPTION:
	case COM_SHUTDOWN:
		err = err || network_mysqld_proto_get_int8(packet, &status);
		if (err) return -1;

		switch (status) {
		case MYSQLD_PACKET_ERR: /* COM_DEBUG may not have the right permissions */
		case MYSQLD_PACKET_EOF:
			is_finished = 1;
			break;
		default:
			g_error("%s.%d: COM_(0x%02x) should be EOF, got x%02x",
					__FILE__, __LINE__,
					parse->command, (guint8)status);
			break;
		}
		break;

	case COM_FIELD_LIST:
		err = err || network_mysqld_proto_get_int8(packet, &status);
		if (err) return -1;

		/* we transfer some data and wait for the EOF */
		switch (status) {
		case MYSQLD_PACKET_ERR:
		case MYSQLD_PACKET_EOF:
			is_finished = 1;
			break;
		case MYSQLD_PACKET_NULL:
		case MYSQLD_PACKET_OK:
			g_error("%s.%d: COM_(0x%02x) should not be (OK|ERR|NULL), got: %02x",
					__FILE__, __LINE__,
					parse->command, status);

			break;
		default:
			break;
		}
		break;
#if MYSQL_VERSION_ID >= 50000
	case COM_STMT_FETCH:
		/*  */
		err = err || network_mysqld_proto_peek_int8(packet, &status);
		if (err) return -1;

		switch (status) {
		case MYSQLD_PACKET_EOF: 
			eof_packet = network_mysqld_eof_packet_new();

			err = err || network_mysqld_proto_get_eof_packet(packet, eof_packet);
			if (!err) {
				if ((eof_packet->server_status & SERVER_STATUS_LAST_ROW_SENT) ||
				    (eof_packet->server_status & SERVER_STATUS_CURSOR_EXISTS)) {
					is_finished = 1;
				}
			}

			network_mysqld_eof_packet_free(eof_packet);

			break; 
		case MYSQLD_PACKET_ERR:
			is_finished = 1;
			break;
		default:
			break;
		}
		break;
#endif
	case COM_QUIT: /* sometimes we get a packet before the connection closes */
	case COM_STATISTICS:
		/* just one packet, no EOF */
		is_finished = 1;

		break;
	case COM_STMT_PREPARE:
		is_finished = network_mysqld_proto_get_com_stmt_prepare_result(packet, parse->data);
		break;
	case COM_STMT_EXECUTE:
		/* COM_STMT_EXECUTE result packets are basically the same as COM_QUERY ones,
		 * the only difference is the encoding of the actual data - fields are in there, too.
		 */
		is_finished = network_mysqld_proto_get_com_query_result(packet, parse->data, con, TRUE);
		break;
	case COM_PROCESS_INFO:
	case COM_QUERY:
#ifdef DEBUG_TRACE_QUERY
		g_debug("now con->current_query is %s\n", con->current_query->str);
#endif
		is_finished = sharding_mysqld_proto_get_com_query_result(packet, parse->data, con, FALSE);
		break;
	case COM_BINLOG_DUMP:
		/**
		 * the binlog-dump event stops, forward all packets as we see them
		 * and keep the command active
		 */
		is_finished = 1;
		break;
	default:
		g_critical("%s: COM_(0x%02x) is not handled", 
				G_STRLOC,
				parse->command);
		err = 1;
		break;
	}

	if (err) return -1;

	return is_finished;
  
}

void sharding_trace_charset(network_mysqld_con* con) {
    sharding_dbgroup_context_t* dbgroup_ctx = con->event_dbgroup_context;
    if (dbgroup_ctx == NULL) { return; }

    GString* charset_client     = dbgroup_ctx->charset_client;
	GString* charset_results    = dbgroup_ctx->charset_results;
	GString* charset_connection = dbgroup_ctx->charset_connection;
    network_socket* server_sock = dbgroup_ctx->backend_sock;

	if (charset_client->len > 0) {
		if (server_sock) g_string_assign_len(server_sock->charset_client, S(charset_client));
		g_string_assign_len(con->client->charset_client, S(charset_client));
	}

	if (charset_results->len > 0) {
		if (server_sock) g_string_assign_len(server_sock->charset_results, S(charset_results));
		g_string_assign_len(con->client->charset_results, S(charset_results));
	}

	if (charset_connection->len > 0) {
		if (server_sock) g_string_assign_len(server_sock->charset_connection, S(charset_connection));
		g_string_assign_len(con->client->charset_connection, S(charset_connection));
	}
}

gint sharding_mysqld_proto_get_com_init_db(network_packet *packet, network_mysqld_com_init_db_result_t *udata, network_mysqld_con *con) 
{
	guint8 status;
	int is_finished;
	int err = 0;
    g_assert(con->event_dbgroup_context != NULL);
    network_socket* server_sock = con->event_dbgroup_context->backend_sock;
	/**
	 * in case we have a init-db statement we track the db-change on the server-side
	 * connection
	 */
	err = err || network_mysqld_proto_get_int8(packet, &status);

	switch (status) {
	case MYSQLD_PACKET_ERR:
		is_finished = 1;
		break;
	case MYSQLD_PACKET_OK:
		/**
		 * track the change of the init_db */
		if (server_sock) g_string_truncate(server_sock->default_db, 0);
		g_string_truncate(con->client->default_db, 0);

		if (udata->db_name && udata->db_name->len) {
			if (server_sock) {
				g_string_append_len(server_sock->default_db, 
						S(udata->db_name));
			}
			
			g_string_append_len(con->client->default_db, 
					S(udata->db_name));
		}
		 
		is_finished = 1;
		break;
	default:
		g_critical("%s.%d: COM_INIT_DB should be (ERR|OK), got %02x",
				__FILE__, __LINE__,
				status);

		return -1;
	}

	if (err) return -1;

	return is_finished;

}

gint sharding_mysqld_proto_get_com_query_result(network_packet *packet, network_mysqld_com_query_result_t *query, network_mysqld_con *con, gboolean use_binary_row_data) {
    int is_finished = 0;
    guint8 status;
    int err = 0;
    network_mysqld_eof_packet_t *eof_packet;
    network_mysqld_ok_packet_t *ok_packet;

    /**
     * if we get a OK in the first packet there will be no result-set
     */
    switch (query->state) {
        case PARSE_COM_QUERY_INIT:
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_ERR: /* e.g. SELECT * FROM dual -> ERROR 1096 (HY000): No tables used */
                    query->query_status = MYSQLD_PACKET_ERR;
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_OK:  /* e.g. DELETE FROM tbl */

                    /**
                     * trace the change of charset
                     */
                    sharding_trace_charset(con); // the diffrent from network_mysqld_proto_get_com_query_result
                    query->query_status = MYSQLD_PACKET_OK;

                    ok_packet = network_mysqld_ok_packet_new();

                    err = err || network_mysqld_proto_get_ok_packet(packet, ok_packet);

                    if (!err) {
                        if (ok_packet->server_status & SERVER_MORE_RESULTS_EXISTS) {

                        } else {
                            is_finished = 1;
                        }

                        query->server_status = ok_packet->server_status;
                        query->warning_count = ok_packet->warnings;
                        query->affected_rows = ok_packet->affected_rows;
                        query->insert_id     = ok_packet->insert_id;
                        query->was_resultset = 0;
                        query->binary_encoded= use_binary_row_data; 
                    }

                    network_mysqld_ok_packet_free(ok_packet);

                    break;
                case MYSQLD_PACKET_NULL:
                    /* OH NO, LOAD DATA INFILE :) */
                    query->state = PARSE_COM_QUERY_LOCAL_INFILE_DATA;
                    is_finished = 1;

                    break;
                case MYSQLD_PACKET_EOF:
                    g_critical("%s: COM_QUERY packet should not be (EOF), got: 0x%02x",
                            G_STRLOC,
                            status);

                    err = 1;

                    break;
                default:
                    query->query_status = MYSQLD_PACKET_OK;
                    /* looks like a result */
                    query->state = PARSE_COM_QUERY_FIELD;
                    break;
            }
            break;
        case PARSE_COM_QUERY_FIELD:
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_ERR:
                case MYSQLD_PACKET_OK:
                case MYSQLD_PACKET_NULL:
                    g_critical("%s: COM_QUERY should not be (OK|NULL|ERR), got: 0x%02x",
                            G_STRLOC,
                            status);

                    err = 1;

                    break;
                case MYSQLD_PACKET_EOF:
                    /**
                     * in 5.0 we have CURSORs which have no rows, just a field definition
                     *
                     * TODO: find a test-case for it, is it COM_STMT_EXECUTE only?
                     */
                    if (packet->data->len == 9) {
                        eof_packet = network_mysqld_eof_packet_new();

                        err = err || network_mysqld_proto_get_eof_packet(packet, eof_packet);

                        if (!err) {
#if MYSQL_VERSION_ID >= 50000
                            /* 5.5 may send a SERVER_MORE_RESULTS_EXISTS as part of the first 
                             * EOF together with SERVER_STATUS_CURSOR_EXISTS. In that case,
                             * we aren't finished. (#61998)
                             *
                             * Only if _CURSOR_EXISTS is set alone, we have a field-definition-only
                             * resultset
                             */
                            if (eof_packet->server_status & SERVER_STATUS_CURSOR_EXISTS &&
                                    !(eof_packet->server_status & SERVER_MORE_RESULTS_EXISTS)) {
                                is_finished = 1;
                            } else {
                                query->state = PARSE_COM_QUERY_RESULT;
                            }
#else
                            query->state = PARSE_COM_QUERY_RESULT;
#endif
                            /* track the server_status of the 1st EOF packet */
                            query->server_status = eof_packet->server_status;
                        }

                        network_mysqld_eof_packet_free(eof_packet);
                    } else {
                        query->state = PARSE_COM_QUERY_RESULT;
                    }
                    break;
                default:
                    break;
            }
            break;
        case PARSE_COM_QUERY_RESULT:
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_EOF:
                    if (packet->data->len == 9) {
                        eof_packet = network_mysqld_eof_packet_new();

                        err = err || network_mysqld_proto_get_eof_packet(packet, eof_packet);

                        if (!err) {
                            query->was_resultset = 1;

#ifndef SERVER_PS_OUT_PARAMS
#define SERVER_PS_OUT_PARAMS 4096
#endif
                            /**
                             * a PS_OUT_PARAMS is set if a COM_STMT_EXECUTE executes a CALL sp(?) where sp is a PROCEDURE with OUT params 
                             *
                             * ...
                             * 05 00 00 12 fe 00 00 0a 10 -- end column-def (auto-commit, more-results, ps-out-params)
                             * ...
                             * 05 00 00 14 fe 00 00 02 00 -- end of rows (auto-commit), see the missing (more-results, ps-out-params)
                             * 07 00 00 15 00 00 00 02 00 00 00 -- OK for the CALL
                             *
                             * for all other resultsets we trust the status-flags of the 2nd EOF packet
                             */
                            if (!(query->server_status & SERVER_PS_OUT_PARAMS)) {
                                query->server_status = eof_packet->server_status;
                            }
                            query->warning_count = eof_packet->warnings;

                            if (query->server_status & SERVER_MORE_RESULTS_EXISTS) {
                                query->state = PARSE_COM_QUERY_INIT;
                            } else {
                                is_finished = 1;
                            }
                        }

                        network_mysqld_eof_packet_free(eof_packet);
                    }

                    break;
                case MYSQLD_PACKET_ERR:
                    /* like 
                     * 
                     * EXPLAIN SELECT * FROM dual; returns an error
                     * 
                     * EXPLAIN SELECT 1 FROM dual; returns a result-set
                     * */
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_OK:
                case MYSQLD_PACKET_NULL:
                    if (use_binary_row_data) {
                        /* fallthrough to default:
                           0x00 is part of the protocol for binary row packets
                           */
                    } else {
                        /* the first field might be a NULL for a text row packet */
                        break;
                    }
                default:
                    query->rows++;
                    query->bytes += packet->data->len;
                    break;
            }
            break;
        case PARSE_COM_QUERY_LOCAL_INFILE_DATA: 
            /* we will receive a empty packet if we are done */
            if (packet->data->len == packet->offset) {
                query->state = PARSE_COM_QUERY_LOCAL_INFILE_RESULT;
                is_finished = 1;
            }
            break;
        case PARSE_COM_QUERY_LOCAL_INFILE_RESULT:
            err = err || network_mysqld_proto_get_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_OK:
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_NULL:
                case MYSQLD_PACKET_ERR:
                case MYSQLD_PACKET_EOF:
                default:
                    g_critical("%s: COM_QUERY,should be (OK), got: 0x%02x",
                            G_STRLOC,
                            status);

                    err = 1;

                    break;
            }

            break;
    }

    if (err) return -1;

    return is_finished;
   
}

static void parse_info_get_tablename(parse_info_t *parse_info, Parse *parse_obj) {
    if (parse_obj == NULL) { return; }

    ParsedResultItem *parsed_item = &parse_obj->parsed.array[0];
    SrcList *srclist = NULL;

    switch(parsed_item->sqltype) {
        case SQLTYPE_SELECT:
            srclist = parsed_item->result.selectObj->pSrc; 
            break;
        case SQLTYPE_INSERT:
        case SQLTYPE_REPLACE:
            srclist = parsed_item->result.insertObj->pTabList;
            break;
        case SQLTYPE_UPDATE:
            srclist = parsed_item->result.updateObj->pTabList;
            break;
        case SQLTYPE_DELETE:
            srclist = parsed_item->result.deleteObj->pTabList;
            break;
        defalut:
            break;
    }
    
    if (srclist && srclist->nSrc > 0) { // only handle the first table name
        parse_info->table_name = srclist->a[0].zName;
        parse_info->db_name = srclist->a[0].zDatabase;
        parse_info->table_token = srclist->a[0].tableToken;
    }
}

void parse_info_init(parse_info_t *parse_info, Parse *parse_obj, const char *sql, guint sql_len) {
    if (parse_info == NULL) return;

    memset(parse_info, 0, sizeof(*parse_info));
    parse_info->parse_obj = parse_obj;
    parse_info_get_tablename(parse_info, parse_obj);
    parse_info->orig_sql = sql;
    parse_info->orig_sql_len = sql_len;
}

Expr* parse_get_where_expr(Parse *parse_obj) {
    ParsedResultItem *parsed_item = &parse_obj->parsed.array[0];
    Expr *where_expr = NULL;

    switch(parsed_item->sqltype) {
        case SQLTYPE_SELECT:
            where_expr = parsed_item->result.selectObj->pWhere; 
            break;
        case SQLTYPE_UPDATE:
            where_expr = parsed_item->result.updateObj->pWhere;
            break;
        case SQLTYPE_DELETE:
            where_expr = parsed_item->result.deleteObj->pWhere;
            break;
        defalut:
            break;
    }
    
    return where_expr;   
}

void dup_token2buff(char *buff, int buff_len, Token token) {
    if (buff == NULL || token.z == NULL) return;
    int cp_size = MIN(buff_len-1, token.n);
    
    memcpy(buff, token.z, cp_size);
    buff[cp_size] = 0;
}

gint parse_get_sql_limit(Parse *parse_obj) {
    gint limit = G_MAXINT;
    if (parse_obj == NULL) { return limit; } 
    ParsedResultItem *parsed_item = &parse_obj->parsed.array[0];
    Expr *limit_expr = NULL;

    switch(parsed_item->sqltype) {
        case SQLTYPE_SELECT:
            limit_expr = parsed_item->result.selectObj->pLimit; 
            break;
        case SQLTYPE_UPDATE:
            limit_expr = parsed_item->result.updateObj->pLimit;
            break;
        case SQLTYPE_DELETE:
            limit_expr = parsed_item->result.deleteObj->pLimit;
            break;
        defalut:
            break;
    }
    if (limit_expr) {
        char limit_str[64] = {0};
        dup_token2buff(limit_str, sizeof(limit_str), limit_expr->token);

        limit = strtoul(limit_str, NULL, 0);
    }   
    return limit;   
}
