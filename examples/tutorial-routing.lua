--[[ $%BEGINLICENSE%$
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

 $%ENDLICENSE%$ --]]

---
-- print the socket information to all IP based routing
--
-- * setup one virtual IP per backend server
-- * let the proxy to all those IPs
-- * create a lua-table that maps a "client.dst.name" to a backend

---
-- print a address 
--
function address_print(prefix, address)
	print(("%s: %s (type = %d, address = %s, port = %d"):format(prefix,
		address.name or "nil",
		address.type or -1,
		address.address or "nil",
		address.port or -1)) -- unix-sockets don't have a port
end

---
-- print the address of the client side
--
function connect_server()
	address_print("client src", proxy.connection.client.src)
	address_print("client dst", proxy.connection.client.dst)
end

---
-- print the address of the connected side and trigger a close of the connection
--
function read_handshake()
	address_print("server src", proxy.connection.server.src)
	address_print("server dst", proxy.connection.server.dst)

	-- tell the client the server denies us
	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = "done"
	}

	return proxy.PROXY_SEND_RESULT
end

