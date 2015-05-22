--[[ $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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

local chassis = require("chassis")
---
-- reply with a single field and row containing an indication if we resolved the client's address.
-- 
-- some fields are not preditable, so we only say "nil" or "not nil"
function read_query( packet )
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = { type = proxy.MYSQLD_PACKET_OK } 
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2)
	local value


	value, errmsg = pcall(loadstring(query))

	if not value then
		chassis.log("critical", query)
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = errmsg
		}
	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ type = proxy.MYSQL_TYPE_STRING, name = "value", },
				},
				rows = { { value } }
			}
		}
	end
	return proxy.PROXY_SEND_RESULT
end

