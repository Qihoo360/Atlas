--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

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
		print("we got a normal query: " .. string.sub(packet, 2))

		proxy.queries:append(1, packet )
		-- generate a new COM_QUERY packet
		--   [ \3SELECT NOW() ]
		-- and inject it with the id = 2
		proxy.queries:append(2, string.char(proxy.COM_QUERY) .. "SELECT NOW()", { resultset_is_needed = true } )

		return proxy.PROXY_SEND_QUERY
	end
end

---
-- read_query_result() is called when we receive a query result 
-- from the server
--
-- we can analyze the response, drop the response and pass it on (default)
--
-- as we injected a SELECT NOW() above, we don't want to tell the client about it
-- and drop the result with proxy.PROXY_IGNORE_RESULT
-- 
-- @return 
--   * nothing or proxy.PROXY_SEND_RESULT to pass the result-set to the client
--   * proxy.PROXY_IGNORE_RESULT to drop the result-set
-- 
function read_query_result(inj)
	print("injected result-set: id = " .. inj.id)

	-- inj.id = 2 was assigned above in the  proxy.queries:append()
	-- 
	-- drop the resultset when we are done to hide it from the client
	-- (in lua the first index is 1, not 0)
	if (inj.id == 2) then
		for row in inj.resultset.rows do
			print("injected query returned: " .. row[1])
		end

		return proxy.PROXY_IGNORE_RESULT
	end
end
