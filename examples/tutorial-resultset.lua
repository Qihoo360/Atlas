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

-- init the query-counter if it isn't done yet
if not proxy.global.query_counter then
	proxy.global.query_counter = 0
end

local query_counter = 0

---
-- read_query() can return a resultset
--
-- You can use read_query() to return a result-set. 
--
-- @param packet the mysql-packet sent by the client
--
-- @return 
--   * nothing to pass on the packet as is, 
--   * proxy.PROXY_SEND_QUERY to send the queries from the proxy.queries queue
--   * proxy.PROXY_SEND_RESULT to send your own result-set
--
function read_query( packet )
	-- a new query came in in this connection
	proxy.global.query_counter = proxy.global.query_counter + 1
	query_counter = query_counter + 1

	if string.byte(packet) == proxy.COM_QUERY then
		--[[

		we use a simple string-match to split commands are word-boundaries
		
		mysql> show querycounter

		is split into 
		command = "show"
		option  = "querycounter"
		
		spaces are ignored, the case has to be as is.

		mysql> show myerror

		returns a error-packet

		--]]
		
		-- try to match the string up to the first non-alphanum
		local f_s, f_e, command = string.find(packet, "^%s*(%w+)", 2)
		local option

		if f_e then
			-- if that match, take the next sub-string as option
			f_s, f_e, option = string.find(packet, "^%s+(%w+)", f_e + 1)
		end
	
		-- we got our commands, execute it
		if string.lower(command) == "show" and string.lower(option) == "querycounter" then
			---
			-- proxy.PROXY_SEND_RESULT requires 
			--
			-- proxy.response.type to be either 
			-- * proxy.MYSQLD_PACKET_OK or
			-- * proxy.MYSQLD_PACKET_ERR
			--
			-- for proxy.MYSQLD_PACKET_OK you need a resultset
			-- * fields
			-- * rows
			--
			-- for proxy.MYSQLD_PACKET_ERR
			-- * errmsg
			proxy.response.type = proxy.MYSQLD_PACKET_OK
			proxy.response.resultset = {
				fields = { 
					{ type = proxy.MYSQL_TYPE_LONG, name = "global_query_counter", },
					{ type = proxy.MYSQL_TYPE_LONG, name = "query_counter", },
				}, 
				rows = { 
					{ proxy.global.query_counter, query_counter }
				}
			}

			-- we have our result, send it back
			return proxy.PROXY_SEND_RESULT
		elseif string.lower(command) == "show" and string.lower(option) == "myerror" then
			proxy.response.type = proxy.MYSQLD_PACKET_ERR
			proxy.response.errmsg = "my first error"
			
			return proxy.PROXY_SEND_RESULT
		end
	end
end


