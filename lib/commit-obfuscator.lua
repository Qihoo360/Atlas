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

--[[

Let a random number of transaction rollback

As part of the QA we have to verify that applications handle 
rollbacks nicely. To simulate a deadlock we turn client-side COMMITs
into server-side ROLLBACKs and return a ERROR packet to the client
telling it a deadlock happened.

--]]

function read_query(packet) 
	if packet:byte() ~= proxy.COM_QUERY then return end

	-- only care about commits 
	if packet:sub(2):upper() ~= "COMMIT" then return end

	-- let 80% fail
	if math.random(10) <= 5 then return end

	proxy.queries:append(1, string.char(proxy.COM_QUERY) .. "ROLLBACK", { resultset_is_needed = true })

	return proxy.PROXY_SEND_QUERY
end

function read_query_result(inj)
	if inj.id ~= 1 then return end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = "Lock wait timeout exceeded; try restarting transaction",
		errno = 1205,
		sqlstate = "HY000"
	}

	return proxy.PROXY_SEND_RESULT
end

