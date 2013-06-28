/* $%BEGINLICENSE%$
 Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.

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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _WIN32
#include <sys/ioctl.h>
#include <sys/socket.h>

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include <arpa/inet.h> /** inet_ntoa */
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <netdb.h>
#include <unistd.h>
#else
#include <winsock2.h>
#include <io.h>
#define ioctl ioctlsocket
#endif

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <glib.h>
#include <glib/gstdio.h>

#include "network-address.h"
#include "glib-ext.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

network_address *network_address_new() {
	network_address *addr;

	addr = g_new0(network_address, 1);
	addr->len = sizeof(addr->addr.common);
	addr->name = g_string_new(NULL);

	return addr;
}

void network_address_free(network_address *addr) {

	if (!addr) return;

#ifndef WIN32
	/*
	 * if the name we're freeing starts with a '/', we're
	 * looking at a unix socket which needs to be removed
	 */
	if (addr->can_unlink_socket == TRUE && addr->name != NULL &&
			addr->name->str != NULL) {
		gchar	*name;
		int		ret;

		name = addr->name->str;
		if (name[0] == '/') {
			ret = g_remove(name);
			if (ret == 0) {
				g_debug("%s: removing socket %s successful", 
					G_STRLOC, name);
			} else {
				if (errno != EPERM && errno != EACCES) {
					g_critical("%s: removing socket %s failed: %s (%d)", 
						G_STRLOC, name, strerror(errno), errno);
				}
			}
		}
	}
#endif /* WIN32 */

	g_string_free(addr->name, TRUE);
	g_free(addr);
}

void network_address_reset(network_address *addr) {
	addr->len = sizeof(addr->addr.common);
}

static gint network_address_set_address_ip(network_address *addr, const gchar *address, guint port) {
	g_return_val_if_fail(addr, -1);

	if (port > 65535) {
		g_critical("%s: illegal value %u for port, only 1 ... 65535 allowed",
				G_STRLOC, port);
		return -1;
	}

	memset(&addr->addr.ipv4, 0, sizeof(struct sockaddr_in));

	if (NULL == address ||
	    strlen(address) == 0 || 
	    0 == strcmp("0.0.0.0", address)) {
		/* no ip */
		addr->addr.ipv4.sin_addr.s_addr = htonl(INADDR_ANY);
		addr->addr.ipv4.sin_family = AF_INET; /* "default" family */
	} else {
#ifdef HAVE_GETADDRINFO
		struct addrinfo *ai = NULL, hint;
		int				rc, family;
		
		memset(&hint, 0, sizeof (hint));
		/*
		 * FIXME: when we add support for IPv6, we'll have to do one
		 * PF_INET* after the other
		 */
		hint.ai_family = PF_INET;
		if ((rc = getaddrinfo(address, NULL, &hint, &ai)) != 0) {
			g_critical("getaddrinfo(%s) failed with %s", address, 
					   gai_strerror(rc));
			return -1;
		}

		do {
			family = ai->ai_family;
#if 0 /* keep this for when we do IPv6 */
			if (family == PF_INET6) {
				memcpy(&addr->addr.ipv6,
						(struct sockaddr_in6 *) ai->ai_addr,
						sizeof (addr->addr.ipv6));
				break;
			} 
#endif /* 0 */
			if (family == PF_INET) {
				memcpy(&addr->addr.ipv4,
						(struct sockaddr_in *) ai->ai_addr, 
						sizeof (addr->addr.ipv4));
				break;
			}
			ai = ai->ai_next;
		} while (NULL != ai);

		if (ai == NULL) {
			/* the family we print here is the *last* ai's */
			g_critical("address %s doesn't resolve to a valid/supported "
					   "address family: %d expected, last found %d", address,
					   PF_INET, family);
			freeaddrinfo(ai);
			return -1;
		}

		freeaddrinfo(ai);
#else 
		struct hostent	*he;
		static GStaticMutex gh_mutex = G_STATIC_MUTEX_INIT;	/*remove lock*/

		g_static_mutex_lock(&gh_mutex);	/*remove lock*/

		he = gethostbyname(address);
		if (NULL == he) {
			g_static_mutex_unlock(&gh_mutex);	/*remove lock*/
			return -1;
		}

		g_assert(he->h_addrtype == AF_INET);
		g_assert(he->h_length == sizeof(struct in_addr));

		memcpy(&(addr->addr.ipv4.sin_addr.s_addr), he->h_addr_list[0], he->h_length);
		g_static_mutex_unlock(&gh_mutex);	/*remove lock*/
		addr->addr.ipv4.sin_family = AF_INET;
#endif /* HAVE_GETADDRINFO */
	}

	addr->addr.ipv4.sin_port = htons(port);
	addr->len = sizeof(struct sockaddr_in);

	(void) network_address_refresh_name(addr);

	return 0;
}

static gint network_address_set_address_un(network_address *addr, const gchar *address) {
	g_return_val_if_fail(addr, -1);
	g_return_val_if_fail(address, -1);

#ifdef HAVE_SYS_UN_H
	if (strlen(address) >= sizeof(addr->addr.un.sun_path) - 1) {
		g_critical("unix-path is too long: %s", address);
		return -1;
	}

	addr->addr.un.sun_family = AF_UNIX;
	strcpy(addr->addr.un.sun_path, address);
	addr->len = sizeof(struct sockaddr_un);
	
	network_address_refresh_name(addr);

	return 0;
#else
	return -1;
#endif
}

/**
 * translate a address-string into a network_address structure
 *
 * - if the address contains a colon we assume IPv4, 
 *   - ":3306" -> (tcp) "0.0.0.0:3306"
 * - if it starts with a / it is a unix-domain socket 
 *   - "/tmp/socket" -> (unix) "/tmp/socket"
 *
 * @param addr     the address-struct
 * @param address  the address string
 * @return 0 on success, -1 otherwise
 */
gint network_address_set_address(network_address *addr, const gchar *address) {
	gchar *s;

	g_return_val_if_fail(addr, -1);

	/* split the address:port */
	if (address[0] == '/')
		return network_address_set_address_un(addr, address);
	
	if (NULL != (s = strchr(address, ':'))) {
		gint ret;
		char *ip_address = g_strndup(address, s - address); /* may be NULL for strdup(..., 0) */
		char *port_err = NULL;

		guint port = strtoul(s + 1, &port_err, 10);

		if (*(s + 1) == '\0') {
			g_critical("%s: IP-address has to be in the form [<ip>][:<port>], is '%s'. No port number",
					G_STRLOC, address);
			ret = -1;
		} else if (*port_err != '\0') {
			g_critical("%s: IP-address has to be in the form [<ip>][:<port>], is '%s'. Failed to parse the port at '%s'",
					G_STRLOC, address, port_err);
			ret = -1;
		} else {
			ret = network_address_set_address_ip(addr, ip_address, port);
		}

		if (ip_address) g_free(ip_address);

		return ret;
	}
	/* perhaps it is a plain IP address, lets add the default-port */
	return network_address_set_address_ip(addr, address, 3306);
}


gint network_address_refresh_name(network_address *addr) {
	/* resolve the peer-addr if we haven't done so yet */
	if (addr->name->len > 0) return 0;

	switch (addr->addr.common.sa_family) {
	case AF_INET:
		g_string_printf(addr->name, "%s:%d", 
				inet_ntoa(addr->addr.ipv4.sin_addr),
				ntohs(addr->addr.ipv4.sin_port));
		break;
#ifdef HAVE_SYS_UN_H
	case AF_UNIX:
		g_string_assign(addr->name, addr->addr.un.sun_path);
		break;
#endif
	default:
        if (addr->addr.common.sa_family > AF_MAX)
            g_debug("%s.%d: ignoring invalid sa_family %d", __FILE__, __LINE__, addr->addr.common.sa_family);
        else
            g_warning("%s.%d: can't convert addr-type %d into a string",
				      __FILE__, __LINE__, 
				      addr->addr.common.sa_family);
		return -1;
	}

	return 0;
}

/**
 * check if the host-part of the address is equal
 */
gboolean network_address_is_local(network_address *dst_addr, network_address *src_addr) {
	if (src_addr->addr.common.sa_family != dst_addr->addr.common.sa_family) {
#ifdef HAVE_SYS_UN_H
		if (src_addr->addr.common.sa_family == AF_UNIX ||
		    dst_addr->addr.common.sa_family == AF_UNIX) {
			/* AF_UNIX is always local,
			 * even if one of the two sides doesn't return a reasonable protocol 
			 *
			 * see #42220
			 */
			return TRUE;
		}
#endif
		g_message("%s: is-local family %d != %d",
				G_STRLOC,
				src_addr->addr.common.sa_family,
				dst_addr->addr.common.sa_family
				);
		return FALSE;
	}

	switch (src_addr->addr.common.sa_family) {
	case AF_INET:
		/* inet_ntoa() returns a pointer to a static buffer
		 * we can't call it twice in the same function-call */

		g_debug("%s: is-local src: %s(:%d) =? ...",
				G_STRLOC,
				inet_ntoa(src_addr->addr.ipv4.sin_addr),
				ntohs(src_addr->addr.ipv4.sin_port));

		g_debug("%s: is-local dst: %s(:%d)",
				G_STRLOC,
				inet_ntoa(dst_addr->addr.ipv4.sin_addr),
				ntohs(dst_addr->addr.ipv4.sin_port)
				);

		return (dst_addr->addr.ipv4.sin_addr.s_addr == src_addr->addr.ipv4.sin_addr.s_addr);
#ifdef HAVE_SYS_UN_H
	case AF_UNIX:
		/* we are always local */
		return TRUE;
#endif
	default:
		g_critical("%s: sa_family = %d", G_STRLOC, src_addr->addr.common.sa_family);
		return FALSE;
	}
}

network_address *network_address_copy(network_address *dst, network_address *src) {
	if (!dst) dst = network_address_new();

	dst->len = src->len;
	dst->addr = src->addr;
	g_string_assign_len(dst->name, S(src->name));

	return dst;
}

