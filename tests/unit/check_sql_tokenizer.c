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
 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>
#include "glib-ext.h"
#include "sql-tokenizer.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

#define START_TEST(x) void (x)(void)
#define END_TEST

/** 
 * tests for the SQL tokenizer
 * @ingroup sql test
 * @{
 */

/**
 * @test check if SQL tokenizing works
 *  
 */
START_TEST(test_tokenizer) {
	GPtrArray *tokens = NULL;
	gsize i;

	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("SELEcT \"qq-end\"\"\", \"\"\"qq-start\", \"'\"`qq-mixed''\" FROM a AS `b`, `ABC``FOO` "));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

#define T(t_id, t_text) \
		g_assert_cmpint(token->token_id, ==, t_id); \
		g_assert_cmpstr(token->text->str, ==, t_text); 

		switch (i) {
		case 0: T(TK_SQL_SELECT, "SELEcT"); break;
		case 1: T(TK_STRING, "qq-end\""); break;
		case 2: T(TK_COMMA, ","); break;
		case 3: T(TK_STRING, "\"qq-start"); break;
		case 4: T(TK_COMMA, ","); break;
		case 5: T(TK_STRING, "'\"`qq-mixed''"); break;
		case 6: T(TK_SQL_FROM, "FROM"); break;
		case 7: T(TK_LITERAL, "a"); break;
		case 8: T(TK_SQL_AS, "AS"); break;
		case 9: T(TK_LITERAL, "b"); break;
		case 10: T(TK_COMMA, ","); break;
		case 11: T(TK_LITERAL, "ABC`FOO"); break;
#undef T
		default:
			 /**
			  * a self-writing test-case 
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			break;
		}
	}

	/* cleanup */
	sql_tokens_free(tokens);
} END_TEST

/**
 * @test table-names might start with a _ even without quoting
 *  
 */
START_TEST(test_table_name_underscore) {
	GPtrArray *tokens = NULL;
	gsize i;

	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("SELEcT * FROM __test_table "));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

#define T(t_id, t_text) \
		g_assert_cmpint(token->token_id, ==, t_id); \
		g_assert_cmpstr(token->text->str, ==, t_text);

		switch (i) {
		case 0: T(TK_SQL_SELECT, "SELEcT"); break;
		case 1: T(TK_STAR, "*"); break;
		case 2: T(TK_SQL_FROM, "FROM"); break;
		case 3: T(TK_LITERAL, "__test_table"); break;
#undef T
		default:
			 /**
			  * a self-writing test-case 
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			break;
		}
	}

	/* cleanup */
	sql_tokens_free(tokens);
} END_TEST


/**
 * @test check if we can map all tokens to a name and back again
 *   
 */
START_TEST(test_token2name) {
	gsize i;

	/* convert tokens to id and back to name */
	for (i = 0; i < TK_LAST_TOKEN; i++) {
		const char *name;

		g_assert((name = sql_token_get_name(i, NULL)));
	}
} END_TEST

/**
 * @test check if single line comments are recognized properly
 */
START_TEST(test_simple_dashdashcomment) {
	gsize i;
	GPtrArray *tokens = NULL;
	
	tokens = sql_tokens_new();
	
	sql_tokenizer(tokens, C("-- comment"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];
		
#define T(t_id, t_text) \
g_assert_cmpint(token->token_id, ==, t_id); \
g_assert_cmpstr(token->text->str, ==, t_text); 

		switch (i) {
		case 0: T(TK_COMMENT, "comment"); break;
		default: g_assert(FALSE); break;
#undef T
		}
	}

	sql_tokens_free(tokens);

} END_TEST

/**
 * @test check if single line comments are recognized properly
 */
START_TEST(test_dashdashcomment) {
	gsize i;
	GPtrArray *tokens = NULL;
	
	tokens = sql_tokens_new();
	
	sql_tokenizer(tokens, C("--  comment\nSELECT 1 FROM dual"));
	
	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];
		
#define T(t_id, t_text) \
g_assert_cmpint(token->token_id, ==, t_id); \
g_assert_cmpstr(token->text->str, ==, t_text); 
		
		switch (i) {
			case 0: T(TK_COMMENT, " comment"); break;	/* note the leading whitespace here! */
			case 1: T(TK_SQL_SELECT, "SELECT"); break;
			case 2: T(TK_INTEGER, "1"); break;
			case 3: T(TK_SQL_FROM, "FROM"); break;
			case 4: T(TK_SQL_DUAL, "dual"); break;
			default: g_assert(FALSE); break;
#undef T
		}
	}
	
	sql_tokens_free(tokens);
	
} END_TEST

/**
 * @test check that '--1' will not start a comment
 */
START_TEST(test_doubleminus) {
	gsize i;
	GPtrArray *tokens = NULL;
	
	tokens = sql_tokens_new();
	
	sql_tokenizer(tokens, C("SELECT 1--1 FROM DUAL"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

#define T(t_id, t_text) \
g_assert_cmpint(token->token_id, ==, t_id); \
g_assert_cmpstr(token->text->str, ==, t_text); 
	
		switch (i) {
			case 0: T(TK_SQL_SELECT, "SELECT"); break;
			case 1: T(TK_INTEGER, "1"); break;
			case 2: T(TK_MINUS, "-"); break;
			case 3: T(TK_MINUS, "-"); break;
			case 4: T(TK_INTEGER, "1"); break;
			case 5: T(TK_SQL_FROM, "FROM"); break;
			case 6: T(TK_SQL_DUAL, "DUAL"); break;
			default: g_assert(FALSE); break;
#undef T
		}
	}	
	
	sql_tokens_free(tokens);
} END_TEST

/**
 * @test First test for bug 36506, where EOF encountered while the tokenizer is in a start start
 *       corrupts its internal state resulting in failure to correctly tokenize the subsequent query.
 */
START_TEST(test_startstate_reset_quoted) {
	gsize i;
	GPtrArray *tokens = NULL;
	
	tokens = sql_tokens_new();
	/* EOF encountered while sql-tokenizer is in QUOTED start state */
	sql_tokenizer(tokens, C("SELECT \"foo"));
	sql_tokens_free(tokens);

	tokens = sql_tokens_new();
	/* valid query, fails with an assertion when bug 36506 is unfixed */
	sql_tokenizer(tokens, C("SELECT \"foo\""));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];
		
#define T(t_id, t_text) \
g_assert_cmpint(token->token_id, ==, t_id); \
g_assert_cmpstr(token->text->str, ==, t_text); 
		
		switch (i) {
			case 0: T(TK_SQL_SELECT, "SELECT"); break;
			case 1: T(TK_STRING, "foo"); break;
			default: g_assert(FALSE); break;
#undef T
		}
	}	
	
	sql_tokens_free(tokens);
	
} END_TEST

/**
 * @test Second test for bug 36506, where EOF encountered while the tokenizer is in a start start
 *       corrupts its internal state resulting in failure to correctly tokenize the subsequent query.
 */
START_TEST(test_startstate_reset_comment) {
	gsize i;
	GPtrArray *tokens = NULL;
	
	/* test for C-style comments */
	
	tokens = sql_tokens_new();
	/* EOF encountered while sql-tokenizer is in COMMENT start state */
	sql_tokenizer(tokens, C("SELECT /* foo"));
	sql_tokens_free(tokens);
	
	tokens = sql_tokens_new();
	/* valid query, fails with an assertion when bug 36506 is unfixed */
	sql_tokenizer(tokens, C("SELECT /*foo*/ 1"));
	
	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];
		
#define T(t_id, t_text) \
g_assert_cmpint(token->token_id, ==, t_id); \
g_assert_cmpstr(token->text->str, ==, t_text); 
		
		switch (i) {
			case 0: T(TK_SQL_SELECT, "SELECT"); break;
			case 1: T(TK_COMMENT, "foo"); break;
			case 2: T(TK_INTEGER, "1"); break;
			default: g_assert(FALSE); break;
#undef T
		}
	}	
	
	sql_tokens_free(tokens);

	/* test for line comments */
	
	tokens = sql_tokens_new();
	/* EOF encountered while sql-tokenizer is in LINECOMMENT start state */
	sql_tokenizer(tokens, C("SELECT -- foo"));
	sql_tokens_free(tokens);
	
	tokens = sql_tokens_new();
	/* valid query, fails with an assertion when bug 36506 is unfixed */
	sql_tokenizer(tokens, C("SELECT -- foo\n1"));
	
	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];
		
#define T(t_id, t_text) \
g_assert_cmpint(token->token_id, ==, t_id); \
g_assert_cmpstr(token->text->str, ==, t_text); 
		
		switch (i) {
			case 0: T(TK_SQL_SELECT, "SELECT"); break;
			case 1: T(TK_COMMENT, "foo"); break;
			case 2: T(TK_INTEGER, "1"); break;
			default: g_assert(FALSE); break;
#undef T
		}
	}	
	
	sql_tokens_free(tokens);
	
} END_TEST
/* @} */

/**
 * get all keywords and try if we get all the ids
 */
void test_tokenizer_keywords() {
	gsize i;

	for (i = 0; sql_token_get_name(i, NULL); i++) {
		const char *keyword;
		size_t keyword_len;

		/** only tokens with TK_SQL_* are keyworks */
		if (0 != strncmp(sql_token_get_name(i, NULL), "TK_SQL_", sizeof("TK_SQL_") - 1)) continue;
		
		keyword = sql_token_get_name(i, &keyword_len);
		/* strip the TK_SQL_ prefix to get the keyword itself */
		keyword += sizeof("TK_SQL_") - 1;
		keyword_len -= sizeof("TK_SQL_") - 1;

		g_assert_cmpint(sql_token_get_id_len(keyword, keyword_len), ==, i);
	}
	
	/* check that some SQL commands are not keywords */
	g_assert_cmpint(sql_token_get_id_len(C("COMMIT")), ==, TK_LITERAL);
	g_assert_cmpint(sql_token_get_id_len(C("TRUNCATE")), ==, TK_LITERAL);
}

/**
 * @test literals can start with a digit, bug#49716
 *
 * - e1 is a literal ("e1")
 * - 1e is a literal ("1e")
 * - 1e + 1 is a literal ("1e"), a plus ("+") and  a int ("1")
 * - 1e+1 is a float ("1e+1")
 * - 1e+1e is a float ("1e+1") and literal ("e") 
 * - 1e1 is a float ("1e1")
 */
void test_literal_digit() {
	GPtrArray *tokens = NULL;
	gsize i;

#define T(t_id, t_text) \
		g_assert_cmpstr(sql_token_get_name(token->token_id, NULL), ==, sql_token_get_name(t_id, NULL)); \
		g_assert_cmpstr(token->text->str, ==, t_text);

	/* e1 is a literal ("e1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("e1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "e1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1e is a literal ("1e") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1e"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "1e"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1e + 1 is a literal ("1e"), a plus ("+") and a integer ("1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1e + 1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "1e"); break;
		case 1: T(TK_PLUS, "+"); break;
		case 2: T(TK_INTEGER, "1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1e+1 is a float ("1e+1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1e+1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_FLOAT, "1e+1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1e1 is a float ("1e1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1e1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_FLOAT, "1e1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1e+1e is a float ("1e+1") and a literal ("e") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1e+1e"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_FLOAT, "1e+1"); break;
		case 1: T(TK_LITERAL, "e"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1.1 is a float ("1.1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1.1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_FLOAT, "1.1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* 1.1e+1 is a float ("1.1e+1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("1.1e+1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_FLOAT, "1.1e+1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* .1t is a float (".1") and a literal ("t")
	 *
	 * the same thing with 'e' is a parse-error in the server side
	 */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C(".1t"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_FLOAT, ".1"); break;
		case 1: T(TK_LITERAL, "t"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* t1.1t is a literal ("t1"), a dot and a literal ("1t") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("t1.1t"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "t1"); break;
		case 1: T(TK_DOT, "."); break;
		case 2: T(TK_LITERAL, "1t"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* e1.1e is a literal ("e1"), a dot and a literal ("1e") ... as 'e' is special */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("e1.1e"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "e1"); break;
		case 1: T(TK_DOT, "."); break;
		case 2: T(TK_LITERAL, "1e"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* e1.1e + 1 is a literal ("e1"), a dot, a literal ("1e"), a plus ("+") and a integer ("1")*/
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("e1.1e + 1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "e1"); break;
		case 1: T(TK_DOT, "."); break;
		case 2: T(TK_LITERAL, "1e"); break;
		case 3: T(TK_PLUS, "+"); break;
		case 4: T(TK_INTEGER, "1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* e1 . 1e + 1 is a literal ("e1"), a dot, a literal ("1e"), a plus ("+") and a integer ("1")*/
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("e1 . 1e + 1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "e1"); break;
		case 1: T(TK_DOT, "."); break;
		case 2: T(TK_LITERAL, "1e"); break;
		case 3: T(TK_PLUS, "+"); break;
		case 4: T(TK_INTEGER, "1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* e1 . 1e+1 is a literal ("e1"), a dot, a float ("1e+1")
	 * ... which is a invalid syntax as fieldnames can't be floats */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("e1 . 1e+1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "e1"); break;
		case 1: T(TK_DOT, "."); break;
		case 2: T(TK_FLOAT, "1e+1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

	/* e1.1e+1 is a literal ("e1"), a dot, a literal ("1e"), a plus ("+") and a integer ("1") */
	tokens = sql_tokens_new();

	sql_tokenizer(tokens, C("e1.1e+1"));

	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		switch (i) {
		case 0: T(TK_LITERAL, "e1"); break;
		case 1: T(TK_DOT, "."); break;
		case 2: T(TK_LITERAL, "1e"); break;
		case 3: T(TK_PLUS, "+"); break;
		case 4: T(TK_INTEGER, "1"); break;
		default:
			 /**
			  * a self-writing test-case
			  */
			printf("case %"G_GSIZE_FORMAT": T(%s, \"%s\"); break;\n", i, sql_token_get_name(token->token_id, NULL), token->text->str);
			g_assert_not_reached();
		}
	}
	sql_tokens_free(tokens);

#undef T

}


int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/tokenizer_keywords", test_tokenizer_keywords);

	g_test_add_func("/core/tokenizer", test_tokenizer);
	g_test_add_func("/core/tokenizer_token2name", test_token2name);
	g_test_add_func("/core/tokenizer_table_name_underscore", test_table_name_underscore);
	g_test_add_func("/core/tokenizer_simple_dashdashcomment", test_simple_dashdashcomment);
	g_test_add_func("/core/tokenizer_dashdashcomment", test_dashdashcomment);
	g_test_add_func("/core/tokenizer_doubleminus", test_doubleminus);
	g_test_add_func("/core/tokenizer_startstate_reset_quoted", test_startstate_reset_quoted);
	g_test_add_func("/core/tokenizer_startstate_reset_comment", test_startstate_reset_comment);

	g_test_add_func("/core/tokenizer_literal_digit", test_literal_digit);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
