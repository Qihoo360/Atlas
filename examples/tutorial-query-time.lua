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
-- getting the query time 
--
-- each injected query we send to the server has a start and end-time
-- 
-- * start-time: when we call proxy.queries:append()
-- * end-time:   when we received the full result-set 
--
-- @param packet the mysql-packet sent by the client
--
-- @return 
--   * proxy.PROXY_SEND_QUERY to send the queries from the proxy.queries queue
--
function read_query( packet )
	if packet:byte() == proxy.COM_QUERY then
		print("we got a normal query: " .. packet:sub(2))

		proxy.queries:append(1, packet, { resultset_is_needed = false} )

		return proxy.PROXY_SEND_QUERY
	end
end

---
-- read_query_result() is called when we receive a query result 
-- from the server
--
-- inj.query_time is the query-time in micro-seconds
-- 
-- @return 
--   * nothing or proxy.PROXY_SEND_RESULT to pass the result-set to the client
-- 
function read_query_result(inj)
	print("query-time: " .. (inj.query_time / 1000) .. "ms")
	print("response-time: " .. (inj.response_time / 1000) .. "ms")
end
