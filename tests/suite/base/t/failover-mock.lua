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
-- test if failover works
--
-- * this script is started twice to simulate two backends
-- * one is shutdown in the test with KILL BACKEND
--

require("chassis") -- 
require("posix") -- 
local proto = require("mysql.proto")

function connect_server()
	-- emulate a server
	proxy.response = {
		type = proxy.MYSQLD_PACKET_RAW,
		packets = {
			proto.to_challenge_packet({})
		}
	}
	return proxy.PROXY_SEND_RESULT
end

if not proxy.global.backend_id then
	proxy.global.backend_id = 0
end

---
-- 
function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		-- just ACK all non COM_QUERY's
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2) 
	local set_id = query:match('SET ID (.)')

	if query == 'GET ID' then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = 'id' },
				},
				rows = { { proxy.global.backend_id } }
			}
		}
	elseif set_id then
		proxy.global.backend_id = set_id
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = 'id' },
				},
				rows = { { proxy.global.backend_id } }
			}
		}
	elseif query == 'KILL BACKEND' then
		-- stop the proxy if we are asked to
		posix.kill(posix.getpid(), 9) -- send SIGKILL to ourself

		-- this won't be sent as we are already dead ... let's hope
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			affected_rows = 0,
			insert_id = 0
		}
	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "(failover-mock) query not handled: " .. query
		}
	end
	return proxy.PROXY_SEND_RESULT
end




