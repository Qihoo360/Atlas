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
local tk = require('proxy.tokenizer')

local DEBUG = os.getenv('DEBUG') or 0
DEBUG=DEBUG+0

function print_debug(msg)
	if DEBUG > 0 then
		print(msg)
	end
end

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


function read_query( packet )
	if packet:byte() ~= proxy.COM_QUERY then 
		print_debug('>>>>>> skipping')
		proxy.response = { type = proxy.MYSQLD_PACKET_OK }
		return proxy.PROXY_SEND_RESULT
	end
	print_debug('>>>>>> after skipping')
	local query = packet:sub(2)
	local tokens = tk.tokenize(query)
	local stripped_tokens = tk.tokens_without_comments(tokens, true )
	local simple_tokens = tk.bare_tokens(stripped_tokens, true )
	proxy.response.type = proxy.MYSQLD_PACKET_OK
	proxy.response.resultset = {
		fields = {
			{ type = proxy.MYSQL_TYPE_STRING, name = "item", },
			{ type = proxy.MYSQL_TYPE_STRING, name = "value", },
		},
		rows = {
			{ 'original', query },
			{ 'rebuilt' , tk.tokens_to_query(tokens) }
		}
	}

	print_debug('>>>>>> returning')
	return proxy.PROXY_SEND_RESULT
end

function disconnect_client()
	print_debug('>>>>>> end session')
end
