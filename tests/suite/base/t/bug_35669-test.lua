--[[ $%BEGINLICENSE%$
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

 $%ENDLICENSE%$ --]]
local proto = require("mysql.proto")

---
-- Bug#35669 is about failing lua scripts should case a error-msg on the MySQL protocol
--
-- we have to jump through some hoops to make this testable with mysqltest as it
-- expects that the initial, default connection always succeeds
--
-- To get this working we use 2 proxy scripts and chain them:
-- 1) ACK the initial connection, forward the 2nd connection to the faulty script
-- 2) loading a faulty script and generate the error-msg
--

function connect_server()
	if not proxy.global.is_faulty then
		-- only ACK mysqltest's default connection
		proxy.response = {
			type = proxy.MYSQLD_PACKET_RAW,
			packets = {
				proto.to_challenge_packet({})
			}
		}
	
		-- the next connection is the faulty connection
		proxy.global.is_faulty = true
		is_initial_connection = true
		return proxy.PROXY_SEND_RESULT
	end
end

---
-- provide a mock function for all commands that mysqltest sends 
-- on the default connection
--
function read_query(packet)
	-- pass on everything that is not on the initial connection
	if not is_initial_connection then return end

	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = "(bug_35669-mock) >" .. packet:sub(2) .. "<"
	}
	return proxy.PROXY_SEND_RESULT
end
