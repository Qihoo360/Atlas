/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

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

#include "chassis-plugin.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

#define START_TEST(x) void (x)(void)
#define END_TEST

/**
 * Tests for the plugin interface
 * @ingroup plugin
 */

/* create a dummy plugin */

struct chassis_plugin_config {
	gchar *foo;
};

chassis_plugin_config *mock_plugin_new(void) {
	chassis_plugin_config *config;

	config = g_new0(chassis_plugin_config, 1);

	return config;
}

void mock_plugin_destroy(chassis_plugin_config *config) {
	if (config->foo) g_free(config->foo);

	g_free(config);
}

GOptionEntry * mock_plugin_get_options(chassis_plugin_config *config) {
	guint i;

	static GOptionEntry config_entries[] = 
	{
		{ "foo", 0, 0, G_OPTION_ARG_STRING, NULL, "foo", "foo" },
		
		{ NULL,  0, 0, G_OPTION_ARG_NONE,   NULL, NULL, NULL }
	};

	i = 0;
	config_entries[i++].arg_data = &(config->foo);

	return config_entries;
}

static void devnull_log_func(const gchar G_GNUC_UNUSED *log_domain, GLogLevelFlags G_GNUC_UNUSED log_level, const gchar G_GNUC_UNUSED *message, gpointer G_GNUC_UNUSED user_data) {
	/* discard the output */
}


/*@{*/

/**
 * load 
 */
START_TEST(test_plugin_load) {
	chassis_plugin *p;
	GLogFunc old_log_func;

	p = chassis_plugin_new();
	g_assert(p != NULL);
	chassis_plugin_free(p);

	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* gtest modifies the fatal-mask */

	old_log_func = g_log_set_default_handler(devnull_log_func, NULL);
	/** should fail */
	p = chassis_plugin_load("non-existing");
	g_log_set_default_handler(old_log_func, NULL);
	g_assert(p == NULL);
	if (p != NULL) chassis_plugin_free(p);
} END_TEST

START_TEST(test_plugin_config) {
	chassis_plugin *p;
	GOptionContext *option_ctx;
	GOptionEntry   *config_entries;
	GOptionGroup *option_grp;
	gchar **_argv;
	int _argc = 2;
	GError *gerr = NULL;
	
	p = chassis_plugin_new();
	p->init = mock_plugin_new;
	p->destroy = mock_plugin_destroy;
	p->get_options = mock_plugin_get_options;

	p->config = p->init();
	g_assert(p->config != NULL);
	
	_argv = g_new(char *, 2);
	_argv[0] = g_strdup("test_plugin_config");
	_argv[1] = g_strdup("--foo=123");

	/* set some config variables */
	option_ctx = g_option_context_new("- MySQL Proxy");

	g_assert(NULL != (config_entries = p->get_options(p->config)));
	
	option_grp = g_option_group_new("foo", "foo-module", "Show options for the foo-module", NULL, NULL);
	g_option_group_add_entries(option_grp, config_entries);
	g_option_context_add_group(option_ctx, option_grp);

	g_assert(FALSE != g_option_context_parse(option_ctx, &_argc, &_argv, &gerr));
	g_option_context_free(option_ctx);

	g_assert(0 == strcmp(p->config->foo, "123"));

	g_free(_argv[1]);

	/* unknown option, let it fail */
	_argv[1] = g_strdup("--fo");
	_argc = 2;

	option_ctx = g_option_context_new("- MySQL Proxy");
	g_assert(NULL != (config_entries = p->get_options(p->config)));
	
	option_grp = g_option_group_new("foo", "foo-module", "Show options for the foo-module", NULL, NULL);
	g_option_group_add_entries(option_grp, config_entries);
	g_option_context_add_group(option_ctx, option_grp);

	g_assert(FALSE == g_option_context_parse(option_ctx, &_argc, &_argv, &gerr));
	g_assert(gerr->domain == G_OPTION_ERROR);
	g_error_free(gerr); gerr = NULL;

	g_option_context_free(option_ctx);

	p->destroy(p->config);
	chassis_plugin_free(p);


	/* let's try again, just let it fail */
} END_TEST
/*@}*/

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/plugin_load", test_plugin_load);
	g_test_add_func("/core/plugin_config", test_plugin_config);

	return g_test_run();
}

#else
int main() {
	return 77;
}
#endif
