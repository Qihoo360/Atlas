--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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

print the statements of all transactions as soon as one of them aborted  

for now we know about:
* Lock wait timeout exceeded
* Deadlock found when trying to get lock

--]]

if not proxy.global.trxs then
	proxy.global.trxs = { }
end

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then return end

	if not proxy.global.trxs[proxy.connection.server.thread_id] then
		proxy.global.trxs[proxy.connection.server.thread_id] = { }
	end

	proxy.queries:append(1, packet, { resultset_is_needed = true })

	local t = proxy.global.trxs[proxy.connection.server.thread_id]

	t[#t + 1] = packet:sub(2)

	return proxy.PROXY_SEND_QUERY
end

function read_query_result(inj)
	local res = inj.resultset
	local flags = res.flags

	if res.query_status == proxy.MYSQLD_PACKET_ERR then
		local err_code     = res.raw:byte(2) + (res.raw:byte(3) * 256)
		local err_sqlstate = res.raw:sub(5, 9)
		local err_msg      = res.raw:sub(10)
		-- print("-- error-packet: " .. err_code)

		if err_code == 1205 or     -- Lock wait timeout exceeded
		   err_code == 1213 then   -- Deadlock found when trying to get lock
			print(("[%d] received a ERR(%d, %s), dumping all active transactions"):format( 
				proxy.connection.server.thread_id,
				err_code,
				err_msg))
			
			for thread_id, statements in pairs(proxy.global.trxs) do
				for stmt_id, statement in ipairs(statements) do
					print(("  [%d].%d: %s"):format(thread_id, stmt_id, statement))
				end
			end
		end
	end

	-- we are done, free the statement list
	if not flags.in_trans then
		proxy.global.trxs[proxy.connection.server.thread_id] = nil
	end
end

function disconnect_client()
	proxy.global.trxs[proxy.connection.server.thread_id] = nil
end

