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

function packet_auth(fields)
	fields = fields or { }
	return "\010" ..             -- proto version
		(fields.version or "5.0.45-proxy") .. -- version
		"\000" ..             -- term-null
		"\001\000\000\000" .. -- thread-id
		"\065\065\065\065" ..
		"\065\065\065\065" .. -- challenge - part I
		"\000" ..             -- filler
		"\001\130" ..         -- server cap (long pass, 4.1 proto)
		"\008" ..             -- charset
		"\002\000" ..         -- status
		("\000"):rep(13) ..   -- filler
		"\065\065\065\065"..
		"\065\065\065\065"..
		"\065\065\065\065"..
		"\000"                -- challenge - part II
end

function connect_server()
	-- emulate a server
	proxy.response = {
		type = proxy.MYSQLD_PACKET_RAW,
		packets = {
			packet_auth()
		}
	}
	return proxy.PROXY_SEND_RESULT
end

function read_query(packet) 
	if packet:byte() == proxy.COM_QUERY then
		local q = packet:sub(2) 

		if q == "SELECT 1" then
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
		elseif q == "SELECT invalid type" then
			-- should be ERR|OK or nil (aka unset)
			
			proxy.response.type = 25
			
			return proxy.PROXY_SEND_RESULT
		elseif q == "SELECT errmsg" then
			-- don't set a errmsg
			
			proxy.response.type = proxy.MYSQLD_PACKET_ERR
			proxy.response.errmsg = "I'm a error"
			
			return proxy.PROXY_SEND_RESULT
		elseif q == "SELECT errmsg empty" then
			-- don't set a errmsg
			
			proxy.response.type = proxy.MYSQLD_PACKET_ERR
			
			return proxy.PROXY_SEND_RESULT
		elseif q == "SELECT errcode" then
			-- don't set a errmsg
			
			proxy.response.type = proxy.MYSQLD_PACKET_ERR
			proxy.response.errmsg = "I'm a error"
			proxy.response.errcode = 1106
			
			return proxy.PROXY_SEND_RESULT
		else
			proxy.response = {
				type = proxy.MYSQLD_PACKET_ERR,
				errmsg = "(raw-packet) unhandled query: " .. q
			}
			
			return proxy.PROXY_SEND_RESULT
		end
	elseif packet:byte() == proxy.COM_INIT_DB then
		local db = packet:sub(2) 

		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			affected_rows = 0,
			insert_id     = 0
		}

		return proxy.PROXY_SEND_RESULT
	end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = "(raw-packet) command " .. tonumber(packet:byte())
	}
			
	return proxy.PROXY_SEND_RESULT
end
