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
local affected_rows

function read_query(packet)
	if packet:byte() == proxy.COM_QUERY then
		if packet:sub(2) == "SELECT affected_rows" then
			proxy.response.type = proxy.MYSQLD_PACKET_OK
			proxy.response.resultset = {
				fields = { 
					{ name = "rows", type = proxy.MYSQL_TYPE_LONG }
				},
				rows = {
					{ affected_rows }
				}
			}
			
			return proxy.PROXY_SEND_RESULT
		else
			proxy.queries:append(1, packet)

			return proxy.PROXY_SEND_QUERY
		end
	end
end

function read_query_result(inj) 
	affected_rows = inj.resultset.affected_rows
end

