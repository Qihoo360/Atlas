/* $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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

#include "network_mysqld_type.h"
#include "glib-ext.h"
#include "string-len.h"

void t_proto_type_date_to_string(void) {
	network_mysqld_type_t *type;
	network_mysqld_type_date_t date;
	const char *const_s;
	char *s;
	gsize s_len;
	char static_s[NETWORK_MYSQLD_TYPE_DATE_MIN_BUF_LEN];

	type = network_mysqld_type_new(MYSQL_TYPE_DATE);

	memset(&date, 0, sizeof(date));
	date.year = 2010;
	date.month = 12;
	date.day = 30;

	date.hour = 19;
	date.min  = 27;
	date.sec  = 30;

	date.nsec = 1;

	g_assert_cmpint(0, ==, network_mysqld_type_set_date(type, &date));
	g_assert_cmpint(-1, ==, network_mysqld_type_get_string_const(type, &const_s, &s_len)); /* it has no string backend, so it can't have a _const for us */

	/* get a copy of the date, alloced */
	s = NULL;
	s_len = 0;
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(10, ==, s_len);
	g_assert_cmpstr("2010-12-30", ==, s);
	g_free(s);

	s = static_s;
	s_len = sizeof(static_s);
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(10, ==, s_len);
	g_assert_cmpstr("2010-12-30", ==, s);

	network_mysqld_type_free(type);
}

void t_proto_type_datetime_to_string(void) {
	network_mysqld_type_t *type;
	network_mysqld_type_date_t date;
	const char *const_s;
	char *s;
	gsize s_len;
	char static_s[NETWORK_MYSQLD_TYPE_DATETIME_MIN_BUF_LEN];

	type = network_mysqld_type_new(MYSQL_TYPE_DATETIME);

	memset(&date, 0, sizeof(date));
	date.year = 2010;
	date.month = 12;
	date.day = 30;

	date.hour = 19;
	date.min  = 27;
	date.sec  = 30;

	date.nsec = 1;

	g_assert_cmpint(0, ==, network_mysqld_type_set_date(type, &date));
	g_assert_cmpint(-1, ==, network_mysqld_type_get_string_const(type, &const_s, &s_len)); /* it has no string backend, so it can't have a _const for us */

	/* get a copy of the date, alloced */
	s = NULL;
	s_len = 0;
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(29, ==, s_len);
	g_assert_cmpstr("2010-12-30 19:27:30.000000001", ==, s);
	g_free(s);

	s = static_s;
	s_len = sizeof(static_s);
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(29, ==, s_len);
	g_assert_cmpstr("2010-12-30 19:27:30.000000001", ==, s);

	network_mysqld_type_free(type);
}

void t_proto_type_timestamp_to_string(void) {
	network_mysqld_type_t *type;
	network_mysqld_type_date_t date;
	const char *const_s;
	char *s;
	gsize s_len;
	char static_s[NETWORK_MYSQLD_TYPE_TIMESTAMP_MIN_BUF_LEN];

	type = network_mysqld_type_new(MYSQL_TYPE_TIMESTAMP);

	memset(&date, 0, sizeof(date));
	date.year = 2010;
	date.month = 12;
	date.day = 30;

	date.hour = 19;
	date.min  = 27;
	date.sec  = 30;

	date.nsec = 1;

	g_assert_cmpint(0, ==, network_mysqld_type_set_date(type, &date));
	g_assert_cmpint(-1, ==, network_mysqld_type_get_string_const(type, &const_s, &s_len)); /* it has no string backend, so it can't have a _const for us */

	/* get a copy of the date, alloced */
	s = NULL;
	s_len = 0;
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(29, ==, s_len);
	g_assert_cmpstr("2010-12-30 19:27:30.000000001", ==, s);
	g_free(s);

	s = static_s;
	s_len = sizeof(static_s);
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(29, ==, s_len);
	g_assert_cmpstr("2010-12-30 19:27:30.000000001", ==, s);

	network_mysqld_type_free(type);
}

void t_proto_type_time_to_string(void) {
	network_mysqld_type_t *type;
	network_mysqld_type_time_t t;
	const char *const_s;
	char *s;
	gsize s_len;
	char static_s[NETWORK_MYSQLD_TYPE_TIME_MIN_BUF_LEN];

	type = network_mysqld_type_new(MYSQL_TYPE_TIME);

	memset(&t, 0, sizeof(t));
	t.sign = -1;
	t.days = 120;
	t.hour = 19;
	t.min  = 27;
	t.sec  = 30;
	t.nsec = 1;

	g_assert_cmpint(0, ==, network_mysqld_type_set_time(type, &t));
	g_assert_cmpint(-1, ==, network_mysqld_type_get_string_const(type, &const_s, &s_len)); /* it has no string backend, so it can't have a _const for us */


	/* get a copy of the time, alloced */
	s = NULL;
	s_len = 0;
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(23, ==, s_len);
	g_assert_cmpstr("-120 19:27:30.000000001", ==, s);
	g_free(s);

	s = static_s;
	s_len = sizeof(static_s);
	g_assert_cmpint(0, ==, network_mysqld_type_get_string(type, &s, &s_len));
	g_assert_cmpint(23, ==, s_len);
	g_assert_cmpstr("-120 19:27:30.000000001", ==, s);

	network_mysqld_type_free(type);
}

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/mysql-proto/type/time", t_proto_type_time_to_string);
	g_test_add_func("/mysql-proto/type/datetime", t_proto_type_datetime_to_string);
	g_test_add_func("/mysql-proto/type/timestamp", t_proto_type_timestamp_to_string);
	g_test_add_func("/mysql-proto/type/date", t_proto_type_date_to_string);

	return g_test_run();
}

