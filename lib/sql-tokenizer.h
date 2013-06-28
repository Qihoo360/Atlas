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
 

#ifndef _SQL_TOKENIZER_H_
#define _SQL_TOKENIZER_H_

#include <glib.h>

/** @file
 *
 * a tokenizer for MySQLs SQL dialect
 *
 * we understand
 * - comparators
 * - strings (double quoted, quoting rules are handled)
 * - literals (quoting of backticks is handled)
 * - keywords
 * - numbers
 */

/**
 * known token ids 
 */
typedef enum {
	TK_UNKNOWN,

	TK_LE,
	TK_GE,
	TK_LT,
	TK_GT,
	TK_EQ,
	TK_NE,

	TK_STRING,
	TK_COMMENT,
	TK_LITERAL,
	TK_FUNCTION,

	TK_INTEGER,
	TK_FLOAT,
	TK_DOT,
	TK_COMMA,

	TK_ASSIGN,
	TK_OBRACE,
	TK_CBRACE,
	TK_SEMICOLON,

	TK_STAR,
	TK_PLUS,
	TK_MINUS,
	TK_DIV,

	TK_BITWISE_AND,
	TK_BITWISE_OR,
	TK_BITWISE_XOR,

	TK_LOGICAL_AND,
	TK_LOGICAL_OR,

	/** a generated list of tokens */
	TK_SQL_ACCESSIBLE,
	TK_SQL_ACTION,
	TK_SQL_ADD,
	TK_SQL_ALL,
	TK_SQL_ALTER,
	TK_SQL_ANALYZE,
	TK_SQL_AND,
	TK_SQL_AS,
	TK_SQL_ASC,
	TK_SQL_ASENSITIVE,
	TK_SQL_BEFORE,
	TK_SQL_BETWEEN,
	TK_SQL_BIGINT,
	TK_SQL_BINARY,
	TK_SQL_BIT,
	TK_SQL_BLOB,
	TK_SQL_BOTH,
	TK_SQL_BY,
	TK_SQL_CALL,
	TK_SQL_CASCADE,
	TK_SQL_CASE,
	TK_SQL_CHANGE,
	TK_SQL_CHAR,
	TK_SQL_CHARACTER,
	TK_SQL_CHECK,
	TK_SQL_COLLATE,
	TK_SQL_COLUMN,
	TK_SQL_CONDITION,
	TK_SQL_CONSTRAINT,
	TK_SQL_CONTINUE,
	TK_SQL_CONVERT,
	TK_SQL_CREATE,
	TK_SQL_CROSS,
	TK_SQL_CURRENT_DATE,
	TK_SQL_CURRENT_TIME,
	TK_SQL_CURRENT_TIMESTAMP,
	TK_SQL_CURRENT_USER,
	TK_SQL_CURSOR,
	TK_SQL_DATABASE,
	TK_SQL_DATABASES,
	TK_SQL_DATE,
	TK_SQL_DAY_HOUR,
	TK_SQL_DAY_MICROSECOND,
	TK_SQL_DAY_MINUTE,
	TK_SQL_DAY_SECOND,
	TK_SQL_DEC,
	TK_SQL_DECIMAL,
	TK_SQL_DECLARE,
	TK_SQL_DEFAULT,
	TK_SQL_DELAYED,
	TK_SQL_DELETE,
	TK_SQL_DESC,
	TK_SQL_DESCRIBE,
	TK_SQL_DETERMINISTIC,
	TK_SQL_DISTINCT,
	TK_SQL_DISTINCTROW,
	TK_SQL_DIV,
	TK_SQL_DOUBLE,
	TK_SQL_DROP,
	TK_SQL_DUAL,
	TK_SQL_EACH,
	TK_SQL_ELSE,
	TK_SQL_ELSEIF,
	TK_SQL_ENCLOSED,
	TK_SQL_ENUM,
	TK_SQL_ESCAPED,
	TK_SQL_EXISTS,
	TK_SQL_EXIT,
	TK_SQL_EXPLAIN,
	TK_SQL_FALSE,
	TK_SQL_FETCH,
	TK_SQL_FLOAT,
	TK_SQL_FLOAT4,
	TK_SQL_FLOAT8,
	TK_SQL_FOR,
	TK_SQL_FORCE,
	TK_SQL_FOREIGN,
	TK_SQL_FROM,
	TK_SQL_FULLTEXT,
	TK_SQL_GRANT,
	TK_SQL_GROUP,
	TK_SQL_HAVING,
	TK_SQL_HIGH_PRIORITY,
	TK_SQL_HOUR_MICROSECOND,
	TK_SQL_HOUR_MINUTE,
	TK_SQL_HOUR_SECOND,
	TK_SQL_IF,
	TK_SQL_IGNORE,
	TK_SQL_IN,
	TK_SQL_INDEX,
	TK_SQL_INFILE,
	TK_SQL_INNER,
	TK_SQL_INOUT,
	TK_SQL_INSENSITIVE,
	TK_SQL_INSERT,
	TK_SQL_INT,
	TK_SQL_INT1,
	TK_SQL_INT2,
	TK_SQL_INT3,
	TK_SQL_INT4,
	TK_SQL_INT8,
	TK_SQL_INTEGER,
	TK_SQL_INTERVAL,
	TK_SQL_INTO,
	TK_SQL_IS,
	TK_SQL_ITERATE,
	TK_SQL_JOIN,
	TK_SQL_KEY,
	TK_SQL_KEYS,
	TK_SQL_KILL,
	TK_SQL_LEADING,
	TK_SQL_LEAVE,
	TK_SQL_LEFT,
	TK_SQL_LIKE,
	TK_SQL_LIMIT,
	TK_SQL_LINEAR,
	TK_SQL_LINES,
	TK_SQL_LOAD,
	TK_SQL_LOCALTIME,
	TK_SQL_LOCALTIMESTAMP,
	TK_SQL_LOCK,
	TK_SQL_LONG,
	TK_SQL_LONGBLOB,
	TK_SQL_LONGTEXT,
	TK_SQL_LOOP,
	TK_SQL_LOW_PRIORITY,
	TK_SQL_MASTER_SSL_VERIFY_SERVER_CERT,
	TK_SQL_MATCH,
	TK_SQL_MEDIUMBLOB,
	TK_SQL_MEDIUMINT,
	TK_SQL_MEDIUMTEXT,
	TK_SQL_MIDDLEINT,
	TK_SQL_MINUTE_MICROSECOND,
	TK_SQL_MINUTE_SECOND,
	TK_SQL_MOD,
	TK_SQL_MODIFIES,
	TK_SQL_NATURAL,
	TK_SQL_NO,
	TK_SQL_NOT,
	TK_SQL_NO_WRITE_TO_BINLOG,
	TK_SQL_NULL,
	TK_SQL_NUMERIC,
	TK_SQL_ON,
	TK_SQL_OPTIMIZE,
	TK_SQL_OPTION,
	TK_SQL_OPTIONALLY,
	TK_SQL_OR,
	TK_SQL_ORDER,
	TK_SQL_OUT,
	TK_SQL_OUTER,
	TK_SQL_OUTFILE,
	TK_SQL_PRECISION,
	TK_SQL_PRIMARY,
	TK_SQL_PROCEDURE,
	TK_SQL_PURGE,
	TK_SQL_RANGE,
	TK_SQL_READ,
	TK_SQL_READ_ONLY,
	TK_SQL_READS,
	TK_SQL_READ_WRITE,
	TK_SQL_REAL,
	TK_SQL_REFERENCES,
	TK_SQL_REGEXP,
	TK_SQL_RELEASE,
	TK_SQL_RENAME,
	TK_SQL_REPEAT,
	TK_SQL_REPLACE,
	TK_SQL_REQUIRE,
	TK_SQL_RESTRICT,
	TK_SQL_RETURN,
	TK_SQL_REVOKE,
	TK_SQL_RIGHT,
	TK_SQL_RLIKE,
	TK_SQL_SCHEMA,
	TK_SQL_SCHEMAS,
	TK_SQL_SECOND_MICROSECOND,
	TK_SQL_SELECT,
	TK_SQL_SENSITIVE,
	TK_SQL_SEPARATOR,
	TK_SQL_SET,
	TK_SQL_SHOW,
	TK_SQL_SMALLINT,
	TK_SQL_SPATIAL,
	TK_SQL_SPECIFIC,
	TK_SQL_SQL,
	TK_SQL_SQL_BIG_RESULT,
	TK_SQL_SQL_CALC_FOUND_ROWS,
	TK_SQL_SQLEXCEPTION,
	TK_SQL_SQL_SMALL_RESULT,
	TK_SQL_SQLSTATE,
	TK_SQL_SQLWARNING,
	TK_SQL_SSL,
	TK_SQL_STARTING,
	TK_SQL_STRAIGHT_JOIN,
	TK_SQL_TABLE,
	TK_SQL_TERMINATED,
	TK_SQL_TEXT,
	TK_SQL_THEN,
	TK_SQL_TIME,
	TK_SQL_TIMESTAMP,
	TK_SQL_TINYBLOB,
	TK_SQL_TINYINT,
	TK_SQL_TINYTEXT,
	TK_SQL_TO,
	TK_SQL_TRAILING,
	TK_SQL_TRIGGER,
	TK_SQL_TRUE,
	TK_SQL_UNDO,
	TK_SQL_UNION,
	TK_SQL_UNIQUE,
	TK_SQL_UNLOCK,
	TK_SQL_UNSIGNED,
	TK_SQL_UPDATE,
	TK_SQL_USAGE,
	TK_SQL_USE,
	TK_SQL_USING,
	TK_SQL_UTC_DATE,
	TK_SQL_UTC_TIME,
	TK_SQL_UTC_TIMESTAMP,
	TK_SQL_VALUES,
	TK_SQL_VARBINARY,
	TK_SQL_VARCHAR,
	TK_SQL_VARCHARACTER,
	TK_SQL_VARYING,
	TK_SQL_WHEN,
	TK_SQL_WHERE,
	TK_SQL_WHILE,
	TK_SQL_WITH,
	TK_SQL_WRITE,
	TK_SQL_X509,
	TK_SQL_XOR,
	TK_SQL_YEAR_MONTH,
	TK_SQL_ZEROFILL,
	
	TK_COMMENT_MYSQL,

	TK_LAST_TOKEN
} sql_token_id;

typedef struct {
	sql_token_id token_id;
	GString *text;
} sql_token;

/** @defgroup sql SQL Tokenizer
 * 
 * SQL tokenizer
 *
 * @code
 *   #define C(s) s, sizeof(s) - 1
 *   GPtrArray *tokens = sql_tokens_new();
 *
 *   if (0 == sql_tokenizer(tokens, C("SELECT 1 FROM tbl"))) {
 *      work_with_the_tokens();
 *   }
 *
 *   sql_tokens_free(tokens);
 * @endcode
 */

/*@{*/

/**
 * create a new sql-token
 * @internal       only used to drive the test-cases 
 *
 * @return         a empty SQL token
 */
sql_token *sql_token_new(void);

/**
 * free a sql-token
 */
void sql_token_free(sql_token *token);

/**
 * get the name for a token-id
 */
const gchar *sql_token_get_name(sql_token_id token_id, size_t *name_len);

/**
 * get the token_id for a literal
 *
 * @internal       only used to drive the test-cases 
 *
 * @param name     a SQL keyword
 * @return         TK_SQL_(keyword) or TK_LITERAL
 */
sql_token_id sql_token_get_id(const gchar *name) G_GNUC_DEPRECATED;

/**
 * get the token_id for a literal
 *
 * @internal       only used to drive the test-cases 
 *
 * @param name     a SQL keyword
 * @return         TK_SQL_(keyword) or TK_LITERAL
 */
sql_token_id sql_token_get_id_len(const gchar *name, size_t name_len);


/**
 * scan a string into SQL tokens
 *
 * @param tokens   a token list to append the tokens too
 * @param str      SQL string to tokenize
 * @param len      length of str
 * @return 0 on success
 *
 */
int sql_tokenizer(GPtrArray *tokens, const gchar *str, gsize len);

/**
 * create a empty token list
 *
 * @note a token list is a GPtrArray *
 *
 * @return a empty token list 
 */
GPtrArray * sql_tokens_new(void);

/**
 * free a token-stream
 *
 * @param tokens   a token list to free
 */
void sql_tokens_free(GPtrArray *tokens);

int sql_token_get_last_id();

/*@}*/

#endif
