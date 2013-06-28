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

---
-- read_query() can rewrite packets
--
-- You can use read_query() to replace the packet sent by the client and rewrite
-- query as you like
--
-- @param packet the mysql-packet sent by the client
--
-- @return 
--   * nothing to pass on the packet as is, 
--   * proxy.PROXY_SEND_QUERY to send the queries from the proxy.queries queue
--   * proxy.PROXY_SEND_RESULT to send your own result-set
--
function read_query( packet )
	if string.byte(packet) == proxy.COM_QUERY then
		local query = string.sub(packet, 2)

		print("we got a normal query: " .. query)

		-- try to match the string up to the first non-alphanum
		local f_s, f_e, command = string.find(packet, "^%s*(%w+)", 2)
		local option

		if f_e then
			-- if that match, take the next sub-string as option
			f_s, f_e, option = string.find(packet, "^%s+(%w+)", f_e + 1)
		end
		
		-- support 
		--
		-- ls [db]
		-- cd db
		-- who

		if command == "ls" then
			if option then
				-- FIXME: SQL INJECTION
				proxy.queries:append(1, string.char(proxy.COM_QUERY) .. "SHOW TABLES FROM " .. option )
			else
				proxy.queries:append(1, string.char(proxy.COM_QUERY) .. "SHOW TABLES" )
			end

			return proxy.PROXY_SEND_QUERY
		elseif command == "who" then
			proxy.queries:append(1, string.char(proxy.COM_QUERY) .. "SHOW PROCESSLIST" )

			return proxy.PROXY_SEND_QUERY
		elseif command == "cd" and option then
			proxy.queries:append(1, string.char(proxy.COM_INIT_DB) .. option )

			return proxy.PROXY_SEND_QUERY
		end
	end
end

---
-- read_query_result() is called when we receive a query result 
-- from the server
--
-- we can analyze the response, drop the response and pass it on (default)
-- 
-- @return 
--   * nothing or proxy.PROXY_SEND_RESULT to pass the result-set to the client
--   * proxy.PROXY_IGNORE_RESULT to drop the result-set
-- 
-- @note: the function has to exist in 0.5.0 if proxy.PROXY_SEND_QUERY 
--   got used in read_query()
--
function read_query_result(inj)
	 
end

