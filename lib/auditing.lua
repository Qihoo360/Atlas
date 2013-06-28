--[[ $%BEGINLICENSE%$
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

 $%ENDLICENSE%$ --]]

local user = ""

function read_handshake( )
	local con = proxy.connection

	print("<-- let's send him some information about us")
	print("    server-addr   : " .. con.server.dst.name)
	print("    client-addr   : " .. con.client.src.name)
end

function read_auth( )
	local con = proxy.connection

	print("--> there, look, the client is responding to the server auth packet")
	print("    username      : " .. con.client.username)

	user = con.client.username
end

function read_auth_result( auth )
	local state = auth.packet:byte()

	if state == proxy.MYSQLD_PACKET_OK then
		print("<-- auth ok");
	elseif state == proxy.MYSQLD_PACKET_ERR then
		print("<-- auth failed");
	else
		print("<-- auth ... don't know: " .. string.format("%q", auth.packet));
	end
end

function read_query( packet ) 
	print("--> '".. user .."' sent us a query")
	if packet:byte() == proxy.COM_QUERY then
		print("    query: " .. packet:sub(2))
	end
end


