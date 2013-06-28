--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2008, Oracle and/or its affiliates. All rights reserved.

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
--[[

   

--]]

-- we need at least 0.5.1
assert(proxy.PROXY_VERSION >= 0x00501, "you need at least mysql-proxy 0.5.1 to run this module")

---
-- read_query() gets the client query before it reaches the server
--
-- @param packet the mysql-packet sent by client
--
-- we have several constants defined, e.g.:
-- * _VERSION
--
-- * proxy.PROXY_VERSION
-- * proxy.COM_* 
-- * proxy.MYSQL_FIELD_*
--
function read_query( packet )
	if packet:byte() == proxy.COM_QUERY then
		print("get got a Query: " .. packet:sub(2))

		-- proxy.PROXY_VERSION is the proxy version as HEX number
		-- 0x00501 is 0.5.1 
		print("we are: " .. string.format("%05x", proxy.PROXY_VERSION))
		print("lua is: " .. _VERSION)
	end
end

