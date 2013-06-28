--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
-- test if we handle connects to a pre-4.1 server nicely
--
local proto = require("mysql.proto")

function packet_auth_40()
	return
		"\010\052\046\048\046\050\056\045\100\101\098\117\103\045\108\111" ..
		"\103\000\005\000\000\000\036\106\107\090\096\086\107\059\000\044" ..
		"\032\008\002\000\000\000\000\000\000\000\000\000\000\000\000\000" ..
		"\000"
end

function packet_auth_50()
	return proto.to_challenge_packet({})
end


proxy.global.connects = proxy.global.connects or 0

function connect_server()
	proxy.response.type = proxy.MYSQLD_PACKET_RAW

	if proxy.global.connects == 0 then
		proxy.response.packets = { packet_auth_50() }
	else
		proxy.response.packets = { packet_auth_40() }

	end
	this_connection_id = proxy.global.connects
	proxy.global.connects = proxy.global.connects + 1
	return proxy.PROXY_SEND_RESULT
end

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2) 
	if query == "SELECT thread_id()" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = { 
					{ name = "thread_id" },
				},
				rows = {
					{ this_connection_id },
				}
			}
		}
	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "(mysql-40-mock) " .. query
		}
	end
	return proxy.PROXY_SEND_RESULT
end




