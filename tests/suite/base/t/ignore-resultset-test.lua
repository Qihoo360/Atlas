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

---
-- test if we can ignore a resultset 
--
function read_query(packet)
	local query = packet:sub(2)

	if query == "ignore_and_replace" then
		packet = string.char(3) .. "SELECT 1"
		proxy.queries:append(1, packet, { resultset_is_needed = true }) -- ignore the resultset
		proxy.queries:append(2, packet, { resultset_is_needed = true }) -- replace the resultset
		return proxy.PROXY_SEND_QUERY
	elseif query == "ignore_and_default" then
		packet = string.char(3) .. "SELECT 1"
		proxy.queries:append(1, packet, { resultset_is_needed = true }) -- ignore the resultset
		proxy.queries:append(3, packet, { resultset_is_needed = true }) -- forward the resulset
		return proxy.PROXY_SEND_QUERY
	end
end

---
-- test if we can replace a resultset after we ignored a first one
function read_query_result(inj)
	if inj.id == 1 then
		return proxy.PROXY_IGNORE_RESULT
	elseif inj.id == 2 then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "1", type = proxy.MYSQL_TYPE_STRING }
				},
				rows = {
					{ "2" }
				}
			}
		}
		return proxy.PROXY_SEND_RESULT
	end
end

