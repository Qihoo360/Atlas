%{
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


#include <string.h>

#include "sql-tokenizer.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <glib-ext.h>

#ifdef WIN32
#include <io.h>  /* for read */
#endif
#include <stdlib.h>

#define YY_DECL int sql_tokenizer_internal(GPtrArray *tokens)

#define GE_STR_LITERAL_WITH_LEN(str) str, sizeof(str) - 1

static void sql_token_append(GPtrArray *tokens, sql_token_id token_id, const gchar *text) G_GNUC_DEPRECATED;
static void sql_token_append_len(GPtrArray *tokens, sql_token_id token_id, const gchar *text, gsize text_len);
static void sql_token_append_last_token_len(GPtrArray *tokens, sql_token_id token_id, const gchar *text, size_t text_len);
static void sql_token_append_last_token(GPtrArray *tokens, sql_token_id token_id, const gchar *text) G_GNUC_DEPRECATED;
sql_token_id sql_token_get_id_len(const gchar *name, gsize name_len);
sql_token_id sql_token_get_id(const gchar *name);

#include "sql-tokenizer-keywords.h" /* generated, brings in sql_keywords */

char quote_char = 0;
sql_token_id quote_token_id = TK_UNKNOWN;
sql_token_id comment_token_id = TK_UNKNOWN;
%}

%option case-insensitive
%option noyywrap
%option never-interactive
%option 8bit
%option fast
%x COMMENT LINECOMMENT QUOTED
%%

	/** comments */
"--"\r?\n       comment_token_id = TK_COMMENT;       sql_token_append_len(tokens, comment_token_id, GE_STR_LITERAL_WITH_LEN(""));
"/*"		comment_token_id = TK_COMMENT;       sql_token_append_len(tokens, comment_token_id, GE_STR_LITERAL_WITH_LEN("")); BEGIN(COMMENT);
"/*!"		comment_token_id = TK_COMMENT_MYSQL; sql_token_append_len(tokens, comment_token_id, GE_STR_LITERAL_WITH_LEN("")); BEGIN(COMMENT);
"--"[[:blank:]]		comment_token_id = TK_COMMENT; sql_token_append_len(tokens, comment_token_id, GE_STR_LITERAL_WITH_LEN("")); BEGIN(LINECOMMENT);
<COMMENT>[^*]*	sql_token_append_last_token_len(tokens, comment_token_id, yytext, yyleng);
<COMMENT>"*"+[^*/]*	sql_token_append_last_token_len(tokens, comment_token_id, yytext, yyleng);
<COMMENT>"*"+"/"	BEGIN(INITIAL);
<COMMENT><<EOF>>	BEGIN(INITIAL);
<LINECOMMENT>[^\n]* sql_token_append_last_token_len(tokens, comment_token_id, yytext, yyleng);
<LINECOMMENT>\r?\n	BEGIN(INITIAL);
<LINECOMMENT><<EOF>>	BEGIN(INITIAL);

	/** start of a quote string */
["'`]		{ BEGIN(QUOTED);  
		quote_char = *yytext; 
		switch (quote_char) { 
		case '\'': quote_token_id = TK_STRING; break; 
		case '"': quote_token_id = TK_STRING; break; 
		case '`': quote_token_id = TK_LITERAL; break; 
		} 
		sql_token_append_len(tokens, quote_token_id, GE_STR_LITERAL_WITH_LEN("")); }
<QUOTED>[^"'`\\]*	sql_token_append_last_token_len(tokens, quote_token_id, yytext, yyleng); /** all non quote or esc chars are passed through */
<QUOTED>"\\".		sql_token_append_last_token_len(tokens, quote_token_id, yytext, yyleng); /** add escaping */
<QUOTED>["'`]{2}	{ if (yytext[0] == yytext[1] && yytext[1] == quote_char) { 
				sql_token_append_last_token_len(tokens, quote_token_id, yytext + 1, yyleng - 1);  /** doubling quotes */
			} else {
				/** pick the first char and put the second back to parsing */
				yyless(1);
				sql_token_append_last_token_len(tokens, quote_token_id, yytext, yyleng);
			}
			}
<QUOTED>["'`]	if (*yytext == quote_char) { BEGIN(INITIAL); } else { sql_token_append_last_token_len(tokens, quote_token_id, yytext, yyleng); }
<QUOTED><<EOF>>	BEGIN(INITIAL);

	/** strings, quoting, literals */
[[:space:]]+	/** ignore WS */

	/* unquoted literals (and function names) are
	 * 
	 *   all alpha-nums that are not digits-only and NOT floats
	 *
	 * Floats are
	 *   1.1
	 *   1e+1
	 *   1.1e+1
	 *   .1e+1
	 * unquoted literals:
	 *   e1
	 *   1e
	 * complex cases
	 *   1e + 1 is a literal ("1e"), a plus ("+") and integer ("1")
	 *   1e+1e  is a float ("1e+1") and a literal ("e")
	 *   compare this to 1.1e which is INVALID (a broken scientific notation)
	 */
([[:digit:]]*".")?[[:digit:]]+[eE][-+]?[[:digit:]]+	sql_token_append_len(tokens, TK_FLOAT, yytext, yyleng);
	/* literals
	 * - be greedy and capture specifiers made up of up to 3 literals: lit.lit.lit
	 * - if it has a dot, split it into 3 tokens: lit dot lit
	 *
	 * when it comes to dots in specifiers spaces matter:
	 *   e1 . 1e + 1
	 *   e1.1e + 1
	 *   e1.1e+1 are all a literal ("e1"), a dot, a literal ("1e"), a plus and a integer ("1")
	 * but 
	 *   e1. 1e+1 is invalid as it is a literal ("e1"), a dot and a float ("1e+1")
	 */
[[:digit:]]*[[:alpha:]_@][[:alnum:]_@]*("."[[:digit:]]*[[:alpha:]_@][[:alnum:]_@]*){0,2}	{
		char *cur, *tk_start = yytext;
		gsize tk_len;

		for (cur = yytext; cur < yytext + yyleng; cur++) {
			if (*cur == '.') {
				tk_len = cur - tk_start;

				sql_token_append_len(tokens, sql_token_get_id_len(tk_start, tk_len), tk_start, tk_len);
				sql_token_append_len(tokens, TK_DOT, GE_STR_LITERAL_WITH_LEN("."));
				tk_start = cur + 1;
			}
		}
		/* copy the rest */
		tk_len = yytext + yyleng - tk_start;
		sql_token_append_len(tokens, sql_token_get_id_len(tk_start, tk_len), tk_start, tk_len);
	}
	/* literals followed by a ( are function names */
[[:digit:]]*[[:alpha:]_@][[:alnum:]_@]*("."[[:digit:]]*[[:alpha:]_@][[:alnum:]_@]*){0,2}\(	 {
		char *cur, *tk_start = yytext;
		gsize tk_len;

		yyless(yyleng - 1); /* on step back to track the parantheses correctly */

		/* split the matched string at the dots */
		for (cur = yytext; cur < yytext + yyleng; cur++) {
			if (*cur == '.') {
				tk_len = cur - tk_start;

				sql_token_append_len(tokens, sql_token_get_id_len(tk_start, tk_len), tk_start, tk_len);
				sql_token_append_len(tokens, TK_DOT, GE_STR_LITERAL_WITH_LEN("."));
				tk_start = cur + 1;
			}
		}
		tk_len = yytext + yyleng - tk_start;
		sql_token_append_len(tokens, TK_FUNCTION, tk_start, tk_len);
	}

[[:digit:]]+	sql_token_append_len(tokens, TK_INTEGER, yytext, yyleng);
[[:digit:]]*"."[[:digit:]]+	sql_token_append_len(tokens, TK_FLOAT, yytext, yyleng);
","		sql_token_append_len(tokens, TK_COMMA, yytext, yyleng);
"."		sql_token_append_len(tokens, TK_DOT, yytext, yyleng);

"<"		sql_token_append_len(tokens, TK_LT, yytext, yyleng);
">"		sql_token_append_len(tokens, TK_GT, yytext, yyleng);
"<="		sql_token_append_len(tokens, TK_LE, yytext, yyleng);
">="		sql_token_append_len(tokens, TK_GE, yytext, yyleng);
"="		sql_token_append_len(tokens, TK_EQ, yytext, yyleng);
"<>"		sql_token_append_len(tokens, TK_NE, yytext, yyleng);
"!="		sql_token_append_len(tokens, TK_NE, yytext, yyleng);

"("		sql_token_append_len(tokens, TK_OBRACE, yytext, yyleng);
")"		sql_token_append_len(tokens, TK_CBRACE, yytext, yyleng);
";"		sql_token_append_len(tokens, TK_SEMICOLON, yytext, yyleng);
":="		sql_token_append_len(tokens, TK_ASSIGN, yytext, yyleng);

"*"		sql_token_append_len(tokens, TK_STAR, yytext, yyleng);
"+"		sql_token_append_len(tokens, TK_PLUS, yytext, yyleng);
"/"		sql_token_append_len(tokens, TK_DIV, yytext, yyleng);
"-"		sql_token_append_len(tokens, TK_MINUS, yytext, yyleng);

"&"		sql_token_append_len(tokens, TK_BITWISE_AND, yytext, yyleng);
"&&"		sql_token_append_len(tokens, TK_LOGICAL_AND, yytext, yyleng);
"|"		sql_token_append_len(tokens, TK_BITWISE_OR, yytext, yyleng);
"||"		sql_token_append_len(tokens, TK_LOGICAL_OR, yytext, yyleng);

"^"		sql_token_append_len(tokens, TK_BITWISE_XOR, yytext, yyleng);

	/** the default rule */
.		sql_token_append_len(tokens, TK_UNKNOWN, yytext, yyleng);

%%
sql_token *sql_token_new(void) {
	sql_token *tk;

	tk = g_new0(sql_token, 1);
	tk->text = g_string_new(NULL);
	tk->token_id = TK_UNKNOWN;

	return tk;
}

/**
 * free a sql-token
 */
void sql_token_free(sql_token *token) {
	if (!token) return;

	g_string_free(token->text, TRUE);
	g_free(token);		
}


/**
 * append a token to the token-list
 */
static void sql_token_append_len(GPtrArray *tokens, sql_token_id token_id, const gchar *text, gsize text_len) {
	sql_token *token;

	token = sql_token_new();
	token->token_id = token_id;
	g_string_assign_len(token->text, text, text_len);

	g_ptr_array_add(tokens, token);
}

static void sql_token_append(GPtrArray *tokens, sql_token_id token_id, const gchar *text) {
	sql_token_append_len(tokens, token_id, text, strlen(text));
}

/**
 * append text to the last token in the token-list
 */
static void sql_token_append_last_token_len(GPtrArray *tokens, sql_token_id token_id, const gchar *text, size_t text_len) {
	sql_token *token;

	g_assert(tokens->len > 0);

	token = tokens->pdata[tokens->len - 1];
	g_assert(token);
	g_assert(token->token_id == token_id);

	g_string_append_len(token->text, text, text_len);
}

static void sql_token_append_last_token(GPtrArray *tokens, sql_token_id token_id, const gchar *text) {
	sql_token_append_last_token_len(tokens, token_id, text, strlen(text));
}

typedef struct {
	const char *name;
	size_t name_len;
} sql_token_cmp_data;

static int sql_token_cmp(const void *_a, const void *_b) {
	int i               = *(int *)_b;
	const sql_token_cmp_data *name    = _a;
	const char *keyword;
	size_t keyword_len;

	keyword = sql_token_get_name(i, &keyword_len);
	g_assert(keyword); /* if this isn't true, we have a internal problem */

       	keyword += sizeof("TK_SQL_") - 1;
       	keyword_len -= sizeof("TK_SQL_") - 1;

	for (i = 0; i < keyword_len && i < name->name_len; i++) {
		int c_diff = g_ascii_tolower(name->name[i]) - g_ascii_tolower(keyword[i]);

		if (0 != c_diff) return c_diff;
	}

	/* we are still here, up to now they are the same */
	return name->name_len - keyword_len;
}

/**
 * get the token_id for a literal 
 */
sql_token_id sql_token_get_id_len(const gchar *name, gsize name_len) {
	gint *i;
	sql_token_cmp_data data;

	/* do a binary search on the sql_keywords */
	data.name = name;
	data.name_len = name_len;

	i = bsearch(&data,
		sql_keywords_get(),
		sql_keywords_get_count(),
		sizeof(int),
		sql_token_cmp);
	
	return i ? *i : TK_LITERAL; /* if we didn't find it, it is literal */
}

/**
 * get the token_id for a literal 
 */
sql_token_id sql_token_get_id(const gchar *name) {
	return sql_token_get_id_len(name, strlen(name));
}

/**
 * scan a string into SQL tokens
 */
int sql_tokenizer(GPtrArray *tokens, const gchar *str, gsize len) {
	YY_BUFFER_STATE state;
	int ret;
	static GStaticMutex mutex = G_STATIC_MUTEX_INIT;

	g_static_mutex_lock(&mutex);
	state = yy_scan_bytes(str, len);
	ret = sql_tokenizer_internal(tokens);
	yy_delete_buffer(state);
	g_static_mutex_unlock(&mutex);

	return ret;
}

GPtrArray *sql_tokens_new(void) {
	return g_ptr_array_new();
}

void sql_tokens_free(GPtrArray *tokens) {
	gsize i;
	for (i = 0; i < tokens->len; i++) {
		sql_token *token = tokens->pdata[i];

		if (token) sql_token_free(token);
	}
	g_ptr_array_free(tokens, TRUE);
}

