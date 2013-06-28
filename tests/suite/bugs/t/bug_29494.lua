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
function read_query(packet) 
	if packet:byte() == proxy.COM_QUERY then
		local q = packet:sub(2) 

		if q == "SELECT 1 /* BUG #29494 */" then
			-- create a packet which is will break the client
			--
			-- HINT: lua uses \ddd (3 decimal digits) instead of octals
			proxy.response.type = proxy.MYSQLD_PACKET_RAW
			proxy.response.packets = {
				"\001",  -- one field
				"\003def" ..   -- catalog
				  "\251" ..  -- db, NULL (crashes client)
				  "\0" ..    -- table
				  "\0" ..    -- orig-table
				  "\0011" .. -- name
				  "\0" ..    -- orig-name
				  "\f" ..    -- filler
				  "\008\0" .. -- charset
				  " \0\0\0" .. -- length
				  "\003" ..    -- type
				  "\002\0" ..  -- flags 
				  "\0" ..    -- decimals
				  "\0\0",    -- filler

				"\254\0\0\002\0", -- EOF
				"\254\0\0\002\0"  -- no data EOF
			}
			
			return proxy.PROXY_SEND_RESULT
		elseif q == "SELECT 1" then
			-- return a empty row
			--
			-- HINT: lua uses \ddd (3 decimal digits) instead of octals
			proxy.response.type = proxy.MYSQLD_PACKET_RAW
			proxy.response.packets = {
				"\001",  -- one field
				"\003def" ..   -- catalog
				  "\0" ..    -- db 
				  "\0" ..    -- table
				  "\0" ..    -- orig-table
				  "\0011" .. -- name
				  "\0" ..    -- orig-name
				  "\f" ..    -- filler
				  "\008\0" .. -- charset
				  " \0\0\0" .. -- length
				  "\003" ..    -- type
				  "\002\0" ..  -- flags 
				  "\0" ..    -- decimals
				  "\0\0",    -- filler

				"\254\0\0\002\0", -- EOF
				"\0011",
				"\254\0\0\002\0"  -- no data EOF
			}
			
			return proxy.PROXY_SEND_RESULT
		end
	end
end
