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
local proto = require("mysql.proto")

---
-- provide a mock function for all commands that mysqltest sends 
-- on the default connection
--
function read_query(packet)
	-- pass on everything that is not on the initial connection

	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	if packet:sub(2) == "REFRESH" then
		proxy.queries:append(1, string.char(7)) -- we don't need the result

		return proxy.PROXY_SEND_QUERY
	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "(bug_41991-test) >" .. packet:sub(2) .. "<"
		}
		return proxy.PROXY_SEND_RESULT
	end
end
