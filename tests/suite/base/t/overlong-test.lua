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

local chassis = assert(require("chassis"))

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	if packet:sub(2) == "SELECT LONG_STRING" then
		-- return a 16M string
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "LONG_STRING", type = proxy.MYSQL_TYPE_STRING },
				},
				rows = { { ("x"):rep(16 * 1024 * 1024 + 1) } }
			}
		}
		return proxy.PROXY_SEND_RESULT
	elseif packet:sub(2, #("SELECT REPLACE") + 1) == "SELECT REPLACE" then
		-- replace the long query by a small one
		proxy.queries:append(1, string.char(3) .. "SELECT \"xxx\"", { resultset_is_needed = false } )
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2, #("SELECT SMALL_QUERY") + 1) == "SELECT SMALL_QUERY" then
		-- replace the small query by a long one
		proxy.queries:append(1, string.char(3) .. "SELECT " .. ("x"):rep(16 * 1024 * 1024 + 1) , { resultset_is_needed = false } )
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2, #("SELECT LENGTH") + 1) == "SELECT LENGTH" then
		-- forward the LENGTH queries AS IS 
		return
	elseif packet:sub(2, #("SELECT RESULT") + 1) == "SELECT RESULT" then
		-- pass this query to the result-set handler
		proxy.queries:append(2, packet, { resultset_is_needed = true } )
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2, #("SELECT SMALL_RESULT") + 1) == "SELECT SMALL_RESULT" then
		-- pass this query to the result-set handler
		proxy.queries:append(2, string.char(3) .. "SELECT " .. ("x"):rep(16 * 1024 * 1024 + 1) , { resultset_is_needed = true } )
		return proxy.PROXY_SEND_QUERY
	else
		-- everything feed through the proxy 
		proxy.queries:append(1, packet, { resultset_is_needed = false } )
		return proxy.PROXY_SEND_QUERY
	end
end

function read_query_result(inj)
	if inj.id == 2 then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "result", type = proxy.MYSQL_TYPE_STRING },
				},
				rows = { { "PASSED"  }  }
			}
		}
		return proxy.PROXY_SEND_RESULT
	end
end

