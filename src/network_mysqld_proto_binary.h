#ifndef __NETWORK_MYSQLD_PROTO_BINARY_H__
#define __NETWORK_MYSQLD_PROTO_BINARY_H__

#include <glib.h>

#include "network-socket.h"
#include "network_mysqld_type.h"

#include "network-exports.h"

NETWORK_API int network_mysqld_proto_binary_get_type(network_packet *packet, network_mysqld_type_t *type);
NETWORK_API int network_mysqld_proto_binary_append_type(GString *packet, network_mysqld_type_t *type);

#endif
