/**
 * Author: gowink
 * email: golangwink@gmail.com
 */
#include <glib.h>
#include "network-conn-pool.h"
#include "network-mysqld-packet.h"

void test_network_connection_pool_new() {
	network_connection_pool *connection_pool = network_connection_pool_new();
	g_assert(connection_pool != NULL);

	network_connection_pool_free(connection_pool);
}

void test_network_connection_pool_add_and_remove() {
	network_connection_pool *connection_pool = network_connection_pool_new();
	g_assert(connection_pool != NULL);
	
	struct network_mysqld_auth_response *mock_auth_response = network_mysqld_auth_response_new();

	network_socket *sock_obj1 = network_socket_new();
	network_socket *sock_obj2 = network_socket_new();
	g_assert(sock_obj1 != NULL && sock_obj2 != NULL);
	sock_obj1->response = mock_auth_response;
	sock_obj2->response = mock_auth_response;

	network_connection_pool_entry *pool_entry1 = network_connection_pool_add(connection_pool, sock_obj1);
	network_connection_pool_entry* pool_entry2 = network_connection_pool_add(connection_pool, sock_obj2);
	g_assert(pool_entry1 != NULL && pool_entry2 != NULL);

	g_assert_cmpint(connection_pool->length, ==, 2);

	network_connection_pool_remove(connection_pool, pool_entry1);
	g_assert_cmpint(connection_pool->length, ==, 1);

	network_connection_pool_remove(connection_pool, pool_entry2);
	g_assert_cmpint(connection_pool->length, ==, 0);

	// no need to free sock , network_connection_pool_remove will do it
	// network_socket_free(sock_obj1);
	// network_socket_free(sock_obj2);
	network_connection_pool_free(connection_pool);
}

void test_network_connection_get() {
	network_connection_pool *connection_pool = network_connection_pool_new();
	g_assert(connection_pool != NULL);
	struct network_mysqld_auth_response *mock_auth_response = network_mysqld_auth_response_new();
	
	network_socket *sock_obj1 = network_socket_new();
	network_socket *sock_obj2 = network_socket_new();
	g_assert(sock_obj1 != NULL && sock_obj2 != NULL);
	sock_obj1->response = mock_auth_response;
	sock_obj2->response = mock_auth_response;

	g_assert(network_connection_pool_add(connection_pool, sock_obj1) != NULL);
	g_assert(network_connection_pool_add(connection_pool, sock_obj2) != NULL);

	g_assert_cmpint(connection_pool->length, ==, 2);
	network_socket *socket_got1 = network_connection_pool_get(connection_pool);
	g_assert(socket_got1 != NULL);
	g_assert(sock_obj1 == socket_got1 || sock_obj2 == socket_got1);

	network_socket *socket_got2 = network_connection_pool_get(connection_pool);
	g_assert(sock_obj1 == socket_got2 || sock_obj2 == socket_got2);
	g_assert_cmpint(connection_pool->length, ==, 0);

	network_socket_free(socket_got1);
	network_socket_free(socket_got2);

	network_connection_pool_free(connection_pool);
}

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");
	
	g_test_add_func("/core/test_network_connection_pool_new", test_network_connection_pool_new);
	g_test_add_func("/core/test_network_connection_pool_add_and_remove", test_network_connection_pool_add_and_remove);
	g_test_add_func("/core/test_network_connection_get", test_network_connection_get);
	return g_test_run();
}
