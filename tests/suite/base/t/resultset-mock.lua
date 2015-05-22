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

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2) 
	if query == 'SELECT 1' then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = '1' },
				},
				rows = { { 1 } }
			}
		}
	elseif query == 'SELECT ' then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '' at line 1",
			sqlstate = "42000",
			errcode = 1064
		}
	elseif query == 'test_res_blob' then
		-- we need a long string, more than 255 chars
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = { 
				fields = { 
					{
						name = "300x",
						type = proxy.MYSQL_TYPE_BLOB
					}
				},
				rows = {
					{ ("x"):rep(300) }
				}
			}
		}
	elseif query == 'SELECT row_count(1), bytes()' then
		-- we need a long string, more than 255 chars
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = { 
				fields = { 
					{ name = "f1" },
					{ name = "f2" },
				},
				rows = {
					{ "1", "2" },
					{ "1", "2" },
				}
			}
		}
	elseif query == 'SELECT "1", NULL, "1"' then
		-- test if we handle NULL fields in the res.rows iterator correctly
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = { 
				fields = { 
					{ name = "f1" },
					{ name = "f2" },
					{ name = "f3" },
				},
				rows = {
					{ "1", nil, "1" },
				}
			}
		}
	elseif query == 'INSERT INTO test.t1 VALUES ( 1 )' then
		-- we need a long string, more than 255 chars
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			affected_rows = 2,
			insert_id = 10
		}
	elseif query == 'SELECT error_msg()' then
		-- we need a long string, more than 255 chars
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "returning SQL-state 42000 and error-code 1064",
			sqlstate = "42000",
			errcode = 1064
		}
	elseif query == 'SELECT 5.0' then
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
	elseif query == 'SELECT 4.1' then
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

			"\254", -- EOF
			"\0011",
			"\254\0\0\002\0"  -- no data EOF
		}
		
		return proxy.PROXY_SEND_RESULT

	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "(resultset-mock) >" .. query .. "<"
		}
	end
	return proxy.PROXY_SEND_RESULT
end




