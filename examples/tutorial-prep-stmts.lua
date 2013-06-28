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

local proto = require("mysql.proto")

local prep_stmts = { }

function read_query( packet )
	local cmd_type = packet:byte()
	if cmd_type == proxy.COM_STMT_PREPARE then
		proxy.queries:append(1, packet, { resultset_is_needed = true } )
		return proxy.PROXY_SEND_QUERY
	elseif cmd_type == proxy.COM_STMT_EXECUTE then
		proxy.queries:append(2, packet, { resultset_is_needed = true } )
		return proxy.PROXY_SEND_QUERY
	elseif cmd_type == proxy.COM_STMT_CLOSE then
		proxy.queries:append(3, packet, { resultset_is_needed = true } )
		return proxy.PROXY_SEND_QUERY
	end
end

function read_query_result(inj) 
	if inj.id == 1 then
		-- print the query we sent
		local stmt_prepare = assert(proto.from_stmt_prepare_packet(inj.query))
		print(("> PREPARE: %s"):format(stmt_prepare.stmt_text))

		-- and the stmt-id we got for it
		if inj.resultset.raw:byte() == 0 then
			local stmt_prepare_ok = assert(proto.from_stmt_prepare_ok_packet(inj.resultset.raw))
			print(("< PREPARE: stmt-id = %d (resultset-cols = %d, params = %d)"):format(
				stmt_prepare_ok.stmt_id,
				stmt_prepare_ok.num_columns,
				stmt_prepare_ok.num_params))

			prep_stmts[stmt_prepare_ok.stmt_id] = {
				num_columns = stmt_prepare_ok.num_columns,
				num_params = stmt_prepare_ok.num_params,
			}
		end
	elseif inj.id == 2 then
		local stmt_id = assert(proto.stmt_id_from_stmt_execute_packet(inj.query))
		local stmt_execute = assert(proto.from_stmt_execute_packet(inj.query, prep_stmts[stmt_id].num_params))
		print(("> EXECUTE: stmt-id = %d"):format(stmt_execute.stmt_id))
		if stmt_execute.new_params_bound then
			for ndx, v in ipairs(stmt_execute.params) do
				print((" [%d] %s (type = %d)"):format(ndx, tostring(v.value), v.type))
			end
		end
	elseif inj.id == 3 then
		local stmt_close = assert(proto.from_stmt_close_packet(inj.query))
		print(("> CLOSE: stmt-id = %d"):format(stmt_close.stmt_id))

		prep_stmts[stmt_close.stmt_id] = nil -- cleanup
	end
end

