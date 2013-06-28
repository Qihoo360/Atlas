/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2008, Oracle and/or its affiliates. All rights reserved.

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
 

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define C(x) x, sizeof(x) - 1

#include <mysql.h>
#include <glib.h>

#include "sys-pedantic.h"

#define TIME_DIFF_US(t2, t1) \
	        ((t2->tv_sec - t1->tv_sec) * 1000000.0 + (t2->tv_usec - t1->tv_usec))

#define START_TIMING(fmt, ...) do { GTimeVal start_ts, stop_ts; GString *ts = g_string_new(NULL); gchar *group = g_strdup_printf(fmt, __VA_ARGS__); g_get_current_time(&start_ts);
#define STOP_TIMING(fmt, ...) g_get_current_time(&stop_ts); g_message("INSERT INTO bench VALUES (\"%s\", \""fmt"\", %s);", group, __VA_ARGS__, get_time_diff_scaled(&stop_ts, &start_ts, ts)); g_string_free(ts, TRUE); g_free(group);} while(0)

#define TEST_QUERY_LARGE    "SELECT REPEAT('x', POW(2, 16)) FROM dual"
#define TEST_QUERY_SMALL    "SELECT 1 FROM dual WHERE 1 = 1"
#define TEST_QUERY_SMALL_PS "SELECT 1 FROM dual WHERE 1 = ?"

gchar * get_time_diff_scaled(GTimeVal *t1, GTimeVal *t2, GString *str) {
	double query_time_us;
	double query_time;
	char *query_time_format = NULL;

	query_time_us = TIME_DIFF_US(t1, t2);

	query_time = query_time_us / 1000.0;
	query_time_format = "%0.3f";

	g_string_printf(str, query_time_format, query_time);

	return str->str;
}
static void log_func(const gchar *UNUSED_PARAM(log_domain), GLogLevelFlags UNUSED_PARAM(log_level), const gchar *message, gpointer UNUSED_PARAM(user_data)) {
	write(STDERR_FILENO, message, strlen(message));
	write(STDERR_FILENO, "\n", 1);
}

int test_prep_stmt(MYSQL *mysql) {
	MYSQL_STMT *stmt;
	long one = 1, two = 0;
	MYSQL_BIND params[1];
	MYSQL_RES *res;
	int round;

	if (NULL == (stmt = mysql_stmt_init(mysql))) {
	       	g_error("%s.%d: mysql_stmt_init() failed: %s", 
				__FILE__, __LINE__, mysql_error(mysql));
	}

	START_TIMING("%s:%d", mysql->host, mysql->port);
	if (mysql_stmt_prepare(stmt, C(TEST_QUERY_SMALL_PS))) {
	       g_error("%s.%d: mysql_stmt_prepare(%s) failed: %s", 
				__FILE__, __LINE__, 
				TEST_QUERY_SMALL_PS,
				mysql_stmt_error(stmt));
	}
	STOP_TIMING("  mysql_stmt_prepare(%s)", "");

	g_assert(mysql_stmt_param_count(stmt) == 1);

	res = mysql_stmt_result_metadata(stmt);
	g_assert(res);
	g_assert(mysql_num_fields(res) == 1);

	params[0].buffer_type = MYSQL_TYPE_LONG;
	params[0].buffer = (char *)&one;
	params[0].is_null = 0;
	params[0].length = 0;

	if (mysql_stmt_bind_param(stmt, params)) {
	       g_error("%s.%d: mysql_stmt_bind_param() failed: %s", 
			       __FILE__, __LINE__, mysql_stmt_error(stmt));
	}

	for (round = 0; round < 2; round++) {
		MYSQL_BIND result[1];
		my_bool is_null;
		my_bool error;
		unsigned long length;

		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (mysql_stmt_execute(stmt)) {
		       	g_error("%s.%d: mysql_stmt_execute() failed: %s", 
					__FILE__, __LINE__, mysql_stmt_error(stmt));
		}
		STOP_TIMING("  mysql_execute(%s)", "");
	
		result[0].buffer_type = MYSQL_TYPE_LONG;
		result[0].buffer = (char *)&two;
		result[0].is_null = &is_null;
		result[0].length = &length;
#if MYSQL_VERSION_ID >= 50000
		result[0].error = &error;
#endif
	
		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (mysql_stmt_bind_result(stmt, result)) {
		       	g_error("%s.%d: mysql_stmt_bind_result() failed: %s", 
					__FILE__, __LINE__, mysql_stmt_error(stmt));
		}
		STOP_TIMING("  mysql_stmt_bind_result(%s)", "");
	
		START_TIMING("%s:%d", mysql->host, mysql->port);
		while (mysql_stmt_fetch(stmt)) {
		}
		STOP_TIMING("  mysql_stmt_fetch(%s)", "");
	}

	mysql_free_result(res);

	if (mysql_stmt_close(stmt)) {
	       	g_error("%s.%d: mysql_stmt_execute() failed: %s", __FILE__, __LINE__, mysql_stmt_error(stmt));
	}

	return 0;
}

int test_prep_stmt_noparam(MYSQL *mysql) {
	MYSQL_STMT *stmt;
	long one = 1, two = 0;
	MYSQL_BIND params[1];
	MYSQL_RES *res;
	int round;

	if (NULL == (stmt = mysql_stmt_init(mysql))) {
	       	g_error("%s.%d: mysql_stmt_init() failed: %s", __FILE__, __LINE__, mysql_error(mysql));
	}

	START_TIMING("%s:%d", mysql->host, mysql->port);
	if (mysql_stmt_prepare(stmt, C(TEST_QUERY_SMALL))) {
	       g_error("%s.%d: mysql_stmt_prepare(%s) failed: %s", 
				__FILE__, __LINE__, 
				TEST_QUERY_SMALL,
				mysql_stmt_error(stmt));
	}
	STOP_TIMING("  mysql_stmt_prepare(%s)", "");

	g_assert(mysql_stmt_param_count(stmt) == 0);

	res = mysql_stmt_result_metadata(stmt);
	g_assert(res);
	g_assert(mysql_num_fields(res) == 1);

	params[0].buffer_type = MYSQL_TYPE_LONG;
	params[0].buffer = (char *)&one;
	params[0].is_null = 0;
	params[0].length = 0;

	if (mysql_stmt_bind_param(stmt, params)) {
	       g_error("%s.%d: mysql_stmt_bind_param() failed: %s", __FILE__, __LINE__, mysql_stmt_error(stmt));
	}

	for (round = 0; round < 2; round++) {
		MYSQL_BIND result[1];
		my_bool is_null;
		my_bool error;
		unsigned long length;

		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (mysql_stmt_execute(stmt)) {
		       	g_error("%s.%d: mysql_stmt_execute() failed: %s", __FILE__, __LINE__, mysql_stmt_error(stmt));
		}
		STOP_TIMING("  mysql_execute(%s)", "");
	
		result[0].buffer_type = MYSQL_TYPE_LONG;
		result[0].buffer = (char *)&two;
		result[0].is_null = &is_null;
		result[0].length = &length;
#if MYSQL_VERSION_ID >= 50000
		result[0].error = &error;
#endif
	
		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (mysql_stmt_bind_result(stmt, result)) {
		       	g_error("%s.%d: mysql_stmt_bind_result() failed: %s", __FILE__, __LINE__, mysql_stmt_error(stmt));
		}
		STOP_TIMING("  mysql_stmt_bind_result(%s)", "");
	
		START_TIMING("%s:%d", mysql->host, mysql->port);
		while (mysql_stmt_fetch(stmt)) {
		}
		STOP_TIMING("  mysql_stmt_fetch(%s)", "");
	}

	mysql_free_result(res);

	if (mysql_stmt_close(stmt)) {
	       	g_error("%s.%d: mysql_stmt_execute() failed: %s", __FILE__, __LINE__, mysql_stmt_error(stmt));
	}

	return 0;
}


int test_select(MYSQL *mysql) {
	MYSQL_RES *res;
	int round;

	for (round = 0; round < 5; round++) {
		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (mysql_real_query(mysql, C(TEST_QUERY_SMALL))) {
		       g_error("%s.%d: mysql_real_query(%s) failed: %s", 
					__FILE__, __LINE__, 
					TEST_QUERY_SMALL,
					mysql_error(mysql));
		}
		STOP_TIMING("  mysql_real_query(%s)", "small");
	
		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (NULL == (res = mysql_store_result(mysql))) {
	 		g_error("%s.%d: mysql_store_result() failed: %s", 
					__FILE__, __LINE__, 
					mysql_error(mysql));
		}
		STOP_TIMING("  mysql_store_result(%s)", "small");

		mysql_free_result(res);
	}
		
	return 0;
}

int test_select_large(MYSQL *mysql) {
	MYSQL_RES *res;
	int round;

	for (round = 0; round < 5; round++) {
		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (mysql_real_query(mysql, C(TEST_QUERY_LARGE))) {
		       g_error("%s.%d: mysql_real_query(%s) failed: %s", 
					__FILE__, __LINE__, 
					TEST_QUERY_LARGE,
					mysql_error(mysql));
		}
		STOP_TIMING("  mysql_real_query(%s)", "large");
	
		START_TIMING("%s:%d", mysql->host, mysql->port);
		if (NULL == (res = mysql_store_result(mysql))) {
	 		g_error("%s.%d: mysql_store_result() failed: %s", 
					__FILE__, __LINE__, 
					mysql_error(mysql));
		}
		STOP_TIMING("  mysql_store_result(%s)", "large");

		mysql_free_result(res);
	}
		
	return 0;
}

int test_server(const gchar *host, guint port, const gchar *user, const gchar *password, const gchar *db) {
	MYSQL *mysql;
	char *indent = " ";

	mysql = mysql_init(NULL);

	START_TIMING("%s:%d", host, port);
	if (!mysql_real_connect(mysql, host, user, password, db, port, NULL, 0)) {
	       	g_error("%s.%d: mysql_real_connect() failed: %s", __FILE__, __LINE__, mysql_error(mysql));
	}
	STOP_TIMING("%smysql_real_connect()", indent);

	START_TIMING("%s:%d", mysql->host, mysql->port);
	test_prep_stmt(mysql);
	STOP_TIMING("%stest_prep_stmt()", indent);

	START_TIMING("%s:%d", mysql->host, mysql->port);
	test_prep_stmt_noparam(mysql);
	STOP_TIMING("%stest_prep_stmt_noparam()", indent);

	START_TIMING("%s:%d", mysql->host, mysql->port);
	test_select(mysql);
	STOP_TIMING("%stest_select()", indent);

	START_TIMING("%s:%d", mysql->host, mysql->port);
	test_select_large(mysql);
	STOP_TIMING("%stest_select_large()", indent);

	mysql->free_me = 1;
	mysql_close(mysql);

	return 0;
}

const gchar *getenv_def(const gchar *key, const gchar *def) {
	const char *p;

	p = getenv(key);

	return p ? p : def;
}

int main(int argc, char **argv) {
	int rounds;
	const gchar *user;
	const gchar *password;
	const gchar *db;
	const gchar *host;

	user     = getenv_def("MYSQL_TEST_USER", "root");	
	password = getenv_def("MYSQL_TEST_PASSWORD", "");	
	db       = getenv_def("MYSQL_TEST_DB",   "test");	
	host     = getenv_def("MYSQL_TEST_HOST", "127.0.0.1");	

	g_log_set_default_handler(log_func, NULL);
#if 0
	/* warm up */
	START_TIMING("%s:%d", "127.0.0.1", 3306);
	test_server("127.0.0.1", 3306);
	STOP_TIMING("warm-up %s", "done");
#endif
	for (rounds = 0; rounds < 10; rounds++) {
		/* real benchmarks */

		START_TIMING("%s:%d", host, 3306);
		test_server(host, 3306, user, password, db);
		STOP_TIMING("benchmark %s", "done");
	
		START_TIMING("%s:%d", host, 4040);
		test_server(host, 4040, user, password, db);
		STOP_TIMING("benchmark %s", "done");
	}

	return 0;
}
